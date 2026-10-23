"""Export a pipeline's Data Commons models as a custom import bundle.

A bundle is what Data Commons ingests: MCF files defining the nodes, CSV files
carrying the observations, and a `config.json` registering both against a
provenance. The dbt models are the interface — every column a model emits is
handed to the dcp-tools call that serialises it, so a pipeline publishes another
property by adding a column rather than by changing this module.

The module itself writes four properties onto every provenance: the pipeline
documentation from the repository's docs directory, as markdown; the provenance
model's `source_release_date` column, as `importedSourceReleaseDate`;
`lastDataRefreshDate`, the export's UTC date, unless the provenance model supplies
one; and `conversionRecipe`, from `ExportSpec.conversion_recipe`. The recipe is a
platform contract. dfo-apis refuses to convert a series whose provenance declares no
recipe, so every export states its recipe, or that it has none, on its spec.
"""

import logging
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from fnmatch import fnmatchcase
from itertools import permutations
from pathlib import Path
from typing import Any

from dcp_tools import CustomDataManager
from dcp_tools.custom_data.models.common import mint_dcid
from duckdb import DuckDBPyConnection
from upath import UPath

from dfo_pipelines.data_commons.mcf import escape_markdown

# The observations are written one file per distinct value of this column. Every
# pipeline's observation model carries the observation date in `date`, so the split
# is a convention of the Data Commons layer rather than a pipeline's choice.
PARTITION_COLUMN = "date"

# Stem of an observation model's files unless its input names another; the files'
# `config.json` pattern derives from it.
OBSERVATION_FILE_STEM = "observations"

# The pipeline documentation kept in the repository, one directory per dataset.
DOCS_ROOT = Path(__file__).resolve().parents[3] / "docs"
# The documentation files every pipeline keeps in its docs directory, keyed on the
# provenance property that carries each one's text.
DOCUMENTATION_FILES = {
    "dataTransformationDescription": "transformations.md",
    "dataTransformationChangelog": "changelog.md",
}

# The provenance property `ExportSpec.conversion_recipe` is written onto.
CONVERSION_RECIPE_PROPERTY = "conversionRecipe"

# The provenance model column every pipeline emits, written onto the provenance as
# its `importedSourceReleaseDate`.
SOURCE_RELEASE_DATE_COLUMN = "source_release_date"


class ConversionRecipe(StrEnum):
    """The members of the ontology's `ConversionRecipeEnum`, as MCF references.

    dfo-apis selects the conversion tables for a series by the `conversionRecipe`
    arc on its provenance.
    """

    DAC = "dcid:ConversionRecipeDAC"
    IMF = "dcid:ConversionRecipeIMF"
    IDS = "dcid:ConversionRecipeIDS"


@dataclass(frozen=True, kw_only=True)
class ObservationInput:
    """An observation model the export writes, registered as one input file.

    Attributes:
        model: Schema-qualified name of the model holding one row per
            observation, written out as one file per distinct value of
            `PARTITION_COLUMN`.
        property_to_column: Maps a Data Commons observation property to the
            model column carrying it.
        file_stem: Stem of the files the model is written to.
    """

    model: str
    property_to_column: dict[str, str]
    file_stem: str = OBSERVATION_FILE_STEM


@dataclass(frozen=True, kw_only=True)
class ExportSpec:
    """What a pipeline supplies to export its Data Commons models as a bundle.

    Attributes:
        import_name: Name of the import, recorded in `config.json`.
        provenance_dcid: Bare token naming the provenance node, e.g.
            `OECDCreditorReportingSystem`, which dcp-tools mints to
            `dcid:provenance/<token>`.
        provenance_model: Schema-qualified name of the model holding exactly one
            row, whose columns are the provenance node's other properties.
        conversion_recipe: Recipe dfo-apis converts the export's series by,
            written onto the provenance as its `conversionRecipe`. None when the
            series are never converted, as for the conversion tables themselves.
            Required, so a new export cannot omit its recipe by accident.
        statvar_model: Schema-qualified name of the model holding one row per
            StatVar, whose columns are the variable's properties.
        observation_inputs: The observation models the export writes, each to
            files of its own, since every observation in an input file shares
            one column mapping.
        source_model: Schema-qualified name of a model holding exactly one row,
            whose columns are the properties of a Source node the import defines.
            Omitted when the provenance's source already exists in Data Commons.
        statvar_column_to_property: Maps a StatVar model column to the ontology
            property whose value it carries.
        docs_dir: Directory holding the pipeline's `DOCUMENTATION_FILES`.
        statvar_ignore_columns: StatVar model columns to leave out of the bundle —
            a column carried for a test or for pipeline bookkeeping rather than
            for publication. Anything not ignored is published, so an internal
            column must be named here.
        statvar_csv_options: Extra keyword arguments passed verbatim to the CSV
            read that serialises the StatVars, e.g. `{"dtype": str}` to keep a
            numeric-looking string column from being coerced to a number.
        constant_observation_properties: Observation properties every observation
            takes the same value of, keyed on the name Data Commons reads (e.g.
            `observationPeriod`), written to `config.json` instead of a column.
        provenance_column_to_property: Maps a provenance model column to an
            ontology property `add_provenance` has no keyword argument for, e.g.
            `deflator_base_year` to `deflatorBaseYear`. The column must hold an
            integer or a string; `export_bundle` raises `ValueError` for anything
            else, since `add_provenance` writes the value into the MCF as given.
            Every other provenance column must be an `add_provenance` keyword.

    Raises:
        ValueError: If `provenance_column_to_property` maps a column onto
            `conversionRecipe`, `importedSourceReleaseDate`, `lastDataRefreshDate`
            or a property the export writes from `DOCUMENTATION_FILES`, if there is
            no observation input, or if one input's file pattern would match
            another's files.
    """

    import_name: str
    provenance_dcid: str
    provenance_model: str
    conversion_recipe: ConversionRecipe | None
    statvar_model: str
    observation_inputs: tuple[ObservationInput, ...]
    source_model: str | None = None
    statvar_column_to_property: dict[str, str]
    docs_dir: Path
    statvar_ignore_columns: tuple[str, ...] = ()
    statvar_csv_options: dict[str, Any] | None = None
    constant_observation_properties: dict[str, str] | None = None
    provenance_column_to_property: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        reserved = DOCUMENTATION_FILES.keys() | {
            CONVERSION_RECIPE_PROPERTY,
            "importedSourceReleaseDate",
            "lastDataRefreshDate",
        }
        clashes = sorted(set(self.provenance_column_to_property.values()) & reserved)
        if clashes:
            raise ValueError(
                f"{self.import_name}: provenance_column_to_property maps onto "
                f"{clashes}, which the export writes from conversion_recipe, "
                f"{SOURCE_RELEASE_DATE_COLUMN}, the export date and the "
                "documentation files"
            )
        if not self.observation_inputs:
            raise ValueError(f"{self.import_name}: no observation inputs")
        stems = [observations.file_stem for observations in self.observation_inputs]
        for stem, other in permutations(stems, 2):
            if other == stem or other.startswith(f"{stem}_"):
                raise ValueError(
                    f"{self.import_name}: the file pattern {_file_pattern(stem)!r} "
                    f"would match the files of {other!r}"
                )

    @property
    def minted_provenance_dcid(self) -> str:
        """The provenance node's dcid as the MCF references it.

        Minted from `provenance_dcid` by the rule `add_provenance` applies, e.g.
        `dcid:provenance/OECDCreditorReportingSystem`.
        """
        return mint_dcid(prefix="provenance", token=self.provenance_dcid)


@dataclass(frozen=True, kw_only=True)
class ExportMetadata:
    """Metadata about the exported bundle.

    Attributes:
        observation_count: Count of observations written across every file.
        statvar_count: Count of StatVars declared in the MCF.
        observation_file_count: Count of files the observations were partitioned into.
    """

    observation_count: int
    statvar_count: int
    observation_file_count: int


def get_model_rows(connection: DuckDBPyConnection, model: str) -> list[dict[str, Any]]:
    """Retrieves every row of a model as a dict, keyed on column name"""
    return connection.sql(f"select * from {model}").to_arrow_table().to_pylist()


def _provenance_property_value(column: str, value: Any) -> str:
    """The MCF text of a `provenance_column_to_property` column's value.

    Raises:
        ValueError: If `value` is neither an integer nor a string.
    """
    # bool is a subclass of int, and True would otherwise be written as "True".
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, str):
        return escape_markdown(value)
    raise ValueError(
        f"Provenance column {column!r} must hold an integer or a string, got {value!r}"
    )


def _file_pattern(stem: str) -> str:
    """Pattern registering every partition file of one observation model"""
    return f"{stem}_*.csv"


def _observation_file_name(stem: str, partition: Any) -> str:
    """Name of the file holding one partition of one observation model"""
    return f"{stem}_{partition}.csv"


def export_bundle(
    spec: ExportSpec,
    *,
    connection: DuckDBPyConnection,
    out_dir: UPath,
    log: logging.Logger,
) -> ExportMetadata:
    """Write the models named by `spec` to `out_dir` as a Data Commons import bundle.

    `export_all` empties `out_dir` before writing the config and the MCF, so the
    bundle carries only what this export wrote — and so the observations are written
    after it, while everything the config records is registered before it.

    Args:
        spec: Models to read, and how their columns map onto Data Commons
            properties.
        connection: Connection to the database holding the models.
        out_dir: Directory the bundle is written to.
        log: Logger to use, e.g. an asset's `context.log`.

    Returns:
        ExportMetadata: Metadata for the exported bundle.

    Raises:
        ValueError: If the provenance or source model does not hold exactly one
            row, the provenance model lacks `source_release_date` or a column
            `spec.provenance_column_to_property` maps, it holds a value in one
            that `_provenance_property_value` refuses, or a written observation
            file matches other than exactly one observation input's pattern.
        FileNotFoundError: If `spec.docs_dir` is missing a documentation file.
    """
    log.info("Generating Data Commons export...")

    log.debug("Creating output directory")
    out_dir.mkdir(parents=True, exist_ok=True)

    manager = CustomDataManager()
    manager.set_import_name(spec.import_name)

    if spec.source_model is not None:
        log.info("Registering Source nodes")
        (source,) = get_model_rows(connection, spec.source_model)
        manager.add_source(**source)

    log.info("Registering Provenance nodes")
    (provenance,) = get_model_rows(connection, spec.provenance_model)
    required = spec.provenance_column_to_property.keys() | {SOURCE_RELEASE_DATE_COLUMN}
    if missing := required - provenance.keys():
        raise ValueError(
            f"{spec.provenance_model} lacks the mapped columns {sorted(missing)}"
        )
    mapped_properties = {
        property_name: _provenance_property_value(column, provenance.pop(column))
        for column, property_name in spec.provenance_column_to_property.items()
    }
    source_release_date = provenance.pop(SOURCE_RELEASE_DATE_COLUMN)
    recipe = (
        {}
        if spec.conversion_recipe is None
        else {CONVERSION_RECIPE_PROPERTY: spec.conversion_recipe.value}
    )
    documentation = {
        name: escape_markdown((spec.docs_dir / file_name).read_text())
        for name, file_name in DOCUMENTATION_FILES.items()
    }
    provenance.setdefault(
        "last_data_refresh_date", datetime.now(UTC).strftime("%Y-%m-%d")
    )
    manager.add_provenance(
        **provenance,
        dcid=spec.provenance_dcid,
        imported_source_release_date=source_release_date,
        additional_properties=recipe | mapped_properties | documentation,
    )

    log.info("Registering StatisticalVariable nodes")
    with tempfile.TemporaryDirectory() as staging_dir:
        statvars_csv = Path(staging_dir) / "statvars.csv"
        (statvar_count,) = connection.execute(
            f"copy (select * from {spec.statvar_model} order by all) to ?"
            " (format csv, header)",
            [str(statvars_csv)],
        ).fetchall()[0]
        manager.add_variables_to_mcf_from_csv(
            statvars_csv,
            column_to_property_mapping=spec.statvar_column_to_property,
            csv_options=spec.statvar_csv_options,
            ignore_columns=list(spec.statvar_ignore_columns),
        )
        manager.add_mcf_file(
            file_name="*.mcf",
            provenance=spec.provenance_dcid,
        )

    log.info("Registering the observation files")
    for observations in spec.observation_inputs:
        manager.add_input_file(
            pattern=_file_pattern(observations.file_stem),
            provenance=spec.provenance_dcid,
            column_mappings=observations.property_to_column,
            observation_properties=spec.constant_observation_properties,
        )

    log.info("Writing MCF files")
    manager.export_all(str(out_dir), validate_data=True)

    log.info("Writing observations")
    observation_count = 0
    observation_file_count = 0
    written_files = []
    for observations in spec.observation_inputs:
        partitions = connection.sql(
            f"""
                select {PARTITION_COLUMN}, count(*)
                from {observations.model}
                group by {PARTITION_COLUMN}
                order by {PARTITION_COLUMN}
                """
        ).fetchall()
        for partition, partition_count in partitions:
            observations_file = _observation_file_name(
                observations.file_stem, partition
            )
            connection.sql(
                f"select * from {observations.model} where {PARTITION_COLUMN} = ?"
                " order by all",
                params=[partition],
            ).write_csv(str(out_dir / observations_file), header=True)
            log.debug(f"Wrote {partition_count} observations to {observations_file}")
            written_files.append(observations_file)
        observation_count += sum(count for _, count in partitions)
        observation_file_count += len(partitions)

    # Each file must be imported once, under its own input's column mapping.
    patterns = [_file_pattern(inputs.file_stem) for inputs in spec.observation_inputs]
    for file_name in written_files:
        matches = [pattern for pattern in patterns if fnmatchcase(file_name, pattern)]
        if len(matches) != 1:
            raise ValueError(
                f"{spec.import_name}: {file_name} matches {matches}, "
                "not exactly one observation pattern"
            )
    log.info(f"Exported to {out_dir}")

    return ExportMetadata(
        observation_count=observation_count,
        statvar_count=statvar_count,
        observation_file_count=observation_file_count,
    )
