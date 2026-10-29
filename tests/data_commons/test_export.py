import dataclasses
import json
import logging
import shutil
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest
from duckdb import DuckDBPyConnection
from upath import UPath

from dfo_pipelines.data_commons.export import (
    DOCUMENTATION_FILES,
    ConversionRecipe,
    ExportMetadata,
    ExportSpec,
    ObservationInput,
    export_bundle,
)

LOG = logging.getLogger(__name__)
FIXTURES = Path(__file__).parent / "fixtures"

ONE_CONSTRAINT_STATVAR = "dcid:Variable1"
TWO_CONSTRAINT_STATVAR = "dcid:Variable2"

OBSERVATIONS = ObservationInput(
    model="test_import.observations",
    property_to_column={
        "variable": "variable",
        "date": "date",
        "value": "value",
        "unit": "unit",
        "custom:entityProperty1": "entity1",
        "custom:entityProperty2": "entity2",
    },
)

EXPORT_SPEC = ExportSpec(
    import_name="test_import",
    provenance_dcid="Provenance1",
    provenance_model="test_import.provenance",
    conversion_recipe=None,
    statvar_model="test_import.statvars",
    observation_inputs=(OBSERVATIONS,),
    statvar_column_to_property={
        "name": "name",
        "constraint1": "constraintProperty1",
        "constraint2": "constraintProperty2",
    },
    docs_dir=FIXTURES / "docs",
)

PROVENANCE_SQL = """
create or replace table test_import.provenance as
select
    'Provenance One' as name,
    'dc/s/Source1' as source,
    'https://example.org/source' as url,
    'https://example.org/source/data.parquet' as source_data_url,
    'https://example.org/terms' as license,
    'dcid:LicenseType1' as license_type,
    'Source One (2026), Provenance One (accessed on 2026-03-04)'
        as license_attribution,
    'P1Y' as source_release_frequency,
    '2026-02-15' as imported_source_release_date,
    '2026-06' as next_source_release_date,
    '2000' as earliest_observation_date,
    '2001' as latest_observation_date
"""

STATVARS_SQL = f"""
create or replace table test_import.statvars as
select * from (values
    (
        '{ONE_CONSTRAINT_STATVAR}',
        'Variable One (constraint one)',
        'dcid:PopulationType1',
        'dcid:measuredProperty1',
        'dcid:measuredValue',
        'constraintProperty1',
        'dcid:ConstraintValue1',
        null,
        'dcid:Qualifier1',
        'entityProperty1,entityProperty2'
    ),
    (
        '{TWO_CONSTRAINT_STATVAR}',
        'Variable Two (constraint one, constraint two)',
        'dcid:PopulationType2',
        'dcid:measuredProperty1',
        'dcid:measuredValue',
        'constraintProperty1,constraintProperty2',
        'dcid:ConstraintValue1',
        'dcid:ConstraintValue2',
        'dcid:Qualifier1',
        'entityProperty1,entityProperty2'
    )
) as statvars (
    dcid,
    name,
    population_type,
    measured_property,
    stat_type,
    constraint_properties,
    constraint1,
    constraint2,
    measurement_qualifier,
    observation_properties
)
"""

OBSERVATIONS_SQL = f"""
create or replace table test_import.observations as
select
    variable,
    date::integer as date,
    entity1,
    entity2,
    value::decimal(18, 5) as value,
    unit
from (values
    ('{ONE_CONSTRAINT_STATVAR}', 2000, 'Entity1', 'Entity2', 1.5, 'Unit1'),
    ('{ONE_CONSTRAINT_STATVAR}', 2001, 'Entity1', 'Entity2', 2.5, 'Unit1'),
    ('{TWO_CONSTRAINT_STATVAR}', 2001, 'Entity3', 'Entity2', 3.5, 'Unit2')
) as observations (variable, date, entity1, entity2, value, unit)
"""


# Observations naming a single entity, which cannot share an input file with the
# observations naming two.
ABOUT_OBSERVATIONS = ObservationInput(
    model="test_import.about_observations",
    property_to_column={
        "variable": "variable",
        "date": "date",
        "value": "value",
        "observationAbout": "observation_about",
    },
    file_stem="about_observations",
)

ABOUT_OBSERVATIONS_SQL = f"""
create or replace table test_import.about_observations as
select variable, date::integer as date, observation_about, value
from (values
    ('{ONE_CONSTRAINT_STATVAR}', 2001, 'Entity4', 4),
    ('{ONE_CONSTRAINT_STATVAR}', 2002, 'Entity4', 5)
) as observations (variable, date, observation_about, value)
"""


def connect(tmp_path: Path) -> DuckDBPyConnection:
    """A database holding the models the export reads, as a small fixture bundle"""
    connection = duckdb.connect(str(tmp_path / "database.duckdb"))
    connection.execute("create schema if not exists test_import")
    connection.execute(PROVENANCE_SQL)
    connection.execute(STATVARS_SQL)
    connection.execute(OBSERVATIONS_SQL)
    connection.execute(ABOUT_OBSERVATIONS_SQL)
    return connection


def run_export(tmp_path: Path) -> tuple[ExportMetadata, Path]:
    """Export the fixture bundle, returning the result and the directory written to"""
    out_dir = UPath(tmp_path / "bundle")
    metadata = export_bundle(
        EXPORT_SPEC, connection=connect(tmp_path), out_dir=out_dir, log=LOG
    )
    return metadata, Path(str(out_dir))


def statvar_nodes(out_dir: Path) -> dict[str, str]:
    """The MCF node written for each StatVar, keyed on its DCID"""
    return {
        node.splitlines()[0].removeprefix("Node: "): node.strip()
        for node in (out_dir / "custom_nodes.mcf").read_text().split("\n\n")
        if node.startswith("Node: ")
    }


def test_export_bundle_writes_a_bundle(tmp_path: Path) -> None:
    """Test the export writes a bundle of MCF, observations and a config"""
    metadata, out_dir = run_export(tmp_path)

    assert sorted(path.name for path in out_dir.iterdir()) == [
        "config.json",
        "custom_nodes.mcf",
        "observations_2000.csv",
        "observations_2001.csv",
        "provenance.mcf",
    ]
    assert metadata == ExportMetadata(
        observation_count=3, statvar_count=2, observation_file_count=2
    )


def test_export_bundle_clears_the_output_directory(tmp_path: Path) -> None:
    """Test the export removes files an earlier export left in the directory"""
    _, out_dir = run_export(tmp_path)
    stale = out_dir / "observations_1999.csv"
    stale.write_text("variable,date,entity1,entity2,value,unit\n")

    metadata, _ = run_export(tmp_path)

    assert not stale.exists()
    assert metadata.observation_file_count == 2


def test_export_bundle_writes_one_observations_file_per_partition(
    tmp_path: Path,
) -> None:
    """Test the export splits the observations on the partition column"""
    _, out_dir = run_export(tmp_path)

    assert (out_dir / "observations_2000.csv").read_text() == (
        "variable,date,entity1,entity2,value,unit\n"
        f"{ONE_CONSTRAINT_STATVAR},2000,Entity1,Entity2,1.50000,Unit1\n"
    )
    assert len((out_dir / "observations_2001.csv").read_text().splitlines()) == 3


def test_export_bundle_writes_the_same_bytes_whatever_the_row_order(
    tmp_path: Path,
) -> None:
    """Test two exports of the same rows, stored in opposite orders, are identical"""
    out_dirs = []
    for name, direction in (("stored_ascending", "asc"), ("stored_descending", "desc")):
        directory = tmp_path / name
        directory.mkdir()
        connection = connect(directory)
        for table in ("test_import.statvars", "test_import.observations"):
            connection.execute(
                f"create or replace table {table} as"
                f" select * from {table} order by all {direction}"
            )
        out_dir = UPath(directory / "bundle")
        export_bundle(EXPORT_SPEC, connection=connection, out_dir=out_dir, log=LOG)
        out_dirs.append(Path(str(out_dir)))

    ascending, descending = out_dirs
    for file_name in ("custom_nodes.mcf", "observations_2001.csv"):
        assert (ascending / file_name).read_bytes() == (
            descending / file_name
        ).read_bytes()


def test_export_bundle_registers_the_observations_by_pattern(tmp_path: Path) -> None:
    """Test the export registers the partition files as one input file"""
    _, out_dir = run_export(tmp_path)

    config = json.loads((out_dir / "config.json").read_text())

    assert config["importName"] == "test_import"
    assert [input_file["pattern"] for input_file in config["inputFiles"]] == [
        "*.mcf",
        "observations_*.csv",
    ]
    mcf, observations = config["inputFiles"]
    assert mcf["provenance"] == EXPORT_SPEC.minted_provenance_dcid
    assert mcf["provenance"] == "dcid:provenance/Provenance1"
    assert observations["provenance"] == mcf["provenance"]
    assert observations["columnMappings"] == {
        "dcid:variableMeasured": "variable",
        "dcid:observationDate": "date",
        "dcid:value": "value",
        "dcid:unit": "unit",
        "custom:entityProperty1": "entity1",
        "custom:entityProperty2": "entity2",
    }


def run_export_with_about_observations(tmp_path: Path) -> tuple[ExportMetadata, Path]:
    """Export the fixture bundle with the single-entity observations added"""
    spec = replace(
        EXPORT_SPEC,
        observation_inputs=(OBSERVATIONS, ABOUT_OBSERVATIONS),
        constant_observation_properties={"observationPeriod": "P1Y"},
    )
    out_dir = UPath(tmp_path / "bundle")
    metadata = export_bundle(
        spec, connection=connect(tmp_path), out_dir=out_dir, log=LOG
    )
    return metadata, Path(str(out_dir))


def test_export_bundle_writes_each_observation_input_to_its_own_files(
    tmp_path: Path,
) -> None:
    """Test each observation model is partitioned into files of its own"""
    metadata, out_dir = run_export_with_about_observations(tmp_path)

    assert sorted(path.name for path in out_dir.glob("*.csv")) == [
        "about_observations_2001.csv",
        "about_observations_2002.csv",
        "observations_2000.csv",
        "observations_2001.csv",
    ]
    assert (out_dir / "about_observations_2001.csv").read_text() == (
        "variable,date,observation_about,value\n"
        f"{ONE_CONSTRAINT_STATVAR},2001,Entity4,4\n"
    )
    assert metadata == ExportMetadata(
        observation_count=5, statvar_count=2, observation_file_count=4
    )


def test_export_bundle_registers_each_observation_input_with_its_own_mapping(
    tmp_path: Path,
) -> None:
    """Test each observation model is its own input file, sharing the constants"""
    _, out_dir = run_export_with_about_observations(tmp_path)

    config = json.loads((out_dir / "config.json").read_text())

    _, observations, about_observations = config["inputFiles"]
    assert about_observations["pattern"] == "about_observations_*.csv"
    assert about_observations["provenance"] == observations["provenance"]
    assert about_observations["columnMappings"] == {
        "dcid:variableMeasured": "variable",
        "dcid:observationDate": "date",
        "dcid:value": "value",
        "dcid:observationAbout": "observation_about",
    }
    assert about_observations["observationProperties"] == {"observationPeriod": "P1Y"}


@pytest.mark.parametrize(
    "file_stems",
    [
        pytest.param(("observations",), id="the_default_stem"),
        pytest.param(("observations_about",), id="extending_the_default_stem"),
        pytest.param(("about", "about"), id="a_repeated_stem"),
        pytest.param(("about", "about_2"), id="extending_another_stem"),
    ],
)
def test_export_spec_refuses_a_file_stem_another_pattern_matches(
    file_stems: tuple[str, ...],
) -> None:
    """Test a spec whose patterns overlap is refused, since one input file would
    also register the other's observations
    """
    with pytest.raises(ValueError, match="would match the files of"):
        replace(
            EXPORT_SPEC,
            observation_inputs=(
                OBSERVATIONS,
                *(replace(ABOUT_OBSERVATIONS, file_stem=stem) for stem in file_stems),
            ),
        )


def test_export_bundle_refuses_a_file_two_observation_patterns_match(
    tmp_path: Path,
) -> None:
    """Test an observation file matched by two inputs' patterns fails the export,
    since the importer would read it under both column mappings
    """
    # A glob in a stem passes the spec's literal overlap check.
    spec = replace(
        EXPORT_SPEC,
        observation_inputs=(
            OBSERVATIONS,
            replace(ABOUT_OBSERVATIONS, file_stem="obs*"),
        ),
    )

    with pytest.raises(ValueError, match=r"observations_2000\.csv matches"):
        export_bundle(
            spec,
            connection=connect(tmp_path),
            out_dir=UPath(tmp_path / "bundle"),
            log=LOG,
        )


def test_export_spec_refuses_no_observation_inputs() -> None:
    """Test a spec must write at least one observation model"""
    with pytest.raises(ValueError, match="no observation inputs"):
        replace(EXPORT_SPEC, observation_inputs=())


def test_export_bundle_writes_constant_observation_properties_to_the_config(
    tmp_path: Path,
) -> None:
    """Test a constant observation property reaches config.json under its DC name"""
    spec = dataclasses.replace(
        EXPORT_SPEC, constant_observation_properties={"observationPeriod": "P1Y"}
    )
    out_dir = UPath(tmp_path / "bundle")
    export_bundle(spec, connection=connect(tmp_path), out_dir=out_dir, log=LOG)

    config = json.loads(Path(str(out_dir / "config.json")).read_text())

    _, observations = config["inputFiles"]
    assert observations["observationProperties"] == {"observationPeriod": "P1Y"}


def test_export_bundle_omits_observation_properties_when_none_are_constant(
    tmp_path: Path,
) -> None:
    """Test an export with no constant observation properties writes none"""
    _, out_dir = run_export(tmp_path)

    config = json.loads((out_dir / "config.json").read_text())

    _, observations = config["inputFiles"]
    assert "observationProperties" not in observations


def test_export_bundle_maps_each_statvar_column_to_its_property(
    tmp_path: Path,
) -> None:
    """Test the export names each StatVar column as the property the spec maps it to"""
    _, out_dir = run_export(tmp_path)

    assert statvar_nodes(out_dir)[TWO_CONSTRAINT_STATVAR] == (
        f"Node: {TWO_CONSTRAINT_STATVAR}\n"
        'name: "Variable Two (constraint one, constraint two)"\n'
        "typeOf: dcid:StatisticalVariable\n"
        "statType: dcid:measuredValue\n"
        "populationType: dcid:PopulationType2\n"
        "measuredProperty: dcid:measuredProperty1\n"
        "constraintProperties: dcid:constraintProperty1, dcid:constraintProperty2\n"
        "measurementQualifier: dcid:Qualifier1\n"
        "observationProperties: dcid:entityProperty1, dcid:entityProperty2\n"
        "constraintProperty1: dcid:ConstraintValue1\n"
        "constraintProperty2: dcid:ConstraintValue2"
    )


def test_export_bundle_omits_a_constraint_the_statvar_does_not_carry(
    tmp_path: Path,
) -> None:
    """Test the export leaves out a constraint whose column is null"""
    _, out_dir = run_export(tmp_path)

    assert statvar_nodes(out_dir)[ONE_CONSTRAINT_STATVAR] == (
        f"Node: {ONE_CONSTRAINT_STATVAR}\n"
        'name: "Variable One (constraint one)"\n'
        "typeOf: dcid:StatisticalVariable\n"
        "statType: dcid:measuredValue\n"
        "populationType: dcid:PopulationType1\n"
        "measuredProperty: dcid:measuredProperty1\n"
        "constraintProperties: dcid:constraintProperty1\n"
        "measurementQualifier: dcid:Qualifier1\n"
        "observationProperties: dcid:entityProperty1, dcid:entityProperty2\n"
        "constraintProperty1: dcid:ConstraintValue1"
    )


def test_export_bundle_skips_a_statvar_column_the_spec_ignores(tmp_path: Path) -> None:
    """Test a column named in statvar_ignore_columns is not published as a property"""
    connection = connect(tmp_path)
    connection.execute(
        "alter table test_import.statvars add column internal_flag boolean"
    )
    connection.execute("update test_import.statvars set internal_flag = true")

    out_dir = UPath(tmp_path / "bundle")
    metadata = export_bundle(
        replace(EXPORT_SPEC, statvar_ignore_columns=("internal_flag",)),
        connection=connection,
        out_dir=out_dir,
        log=LOG,
    )

    assert metadata.statvar_count == 2
    assert "internal_flag" not in (out_dir / "custom_nodes.mcf").read_text()


def test_export_bundle_does_not_coerce_a_numeric_looking_string(
    tmp_path: Path,
) -> None:
    """Test a string column holding a number stays a string through the CSV round-trip"""
    connection = connect(tmp_path)
    connection.execute(
        "alter table test_import.statvars add column scaling_factor varchar"
    )
    connection.execute(
        """
        update test_import.statvars set scaling_factor = '100'
        where dcid = ?
        """,
        [ONE_CONSTRAINT_STATVAR],
    )

    out_dir = UPath(tmp_path / "bundle")
    export_bundle(
        replace(
            EXPORT_SPEC,
            statvar_column_to_property={
                **EXPORT_SPEC.statvar_column_to_property,
                "scaling_factor": "scalingFactor",
            },
            statvar_csv_options={"dtype": str},
        ),
        connection=connection,
        out_dir=out_dir,
        log=LOG,
    )

    mcf = (out_dir / "custom_nodes.mcf").read_text()
    assert "scalingFactor: 100\n" in mcf
    assert "100.0" not in mcf


def test_export_bundle_writes_the_provenance_from_the_spec_and_model(
    tmp_path: Path,
) -> None:
    """Test the provenance node takes its dcid from the spec, its refresh date from the export's UTC clock, and the rest from the model"""
    with patch("dfo_pipelines.data_commons.export.datetime") as mock_datetime:
        # The local date differs, so a clock read in local time fails the test.
        mock_datetime.now.side_effect = lambda tz=None: (
            datetime(2026, 10, 14, tzinfo=UTC) if tz is UTC else datetime(2026, 10, 13)
        )
        _, out_dir = run_export(tmp_path)

    assert (out_dir / "provenance.mcf").read_text().strip() == (
        "Node: dcid:provenance/Provenance1\n"
        'name: "Provenance One"\n'
        "typeOf: dcid:Provenance\n"
        'url: "https://example.org/source"\n'
        "source: dcid:dc/s/Source1\n"
        'sourceDataUrl: "https://example.org/source/data.parquet"\n'
        'license: "https://example.org/terms"\n'
        "licenseType: dcid:LicenseType1\n"
        'licenseAttribution: "Source One (2026), Provenance One '
        '(accessed on 2026-03-04)"\n'
        'lastDataRefreshDate: "2026-10-14"\n'
        'importedSourceReleaseDate: "2026-02-15"\n'
        'nextSourceReleaseDate: "2026-06"\n'
        'sourceReleaseFrequency: "P1Y"\n'
        'earliestObservationDate: "2000"\n'
        'latestObservationDate: "2001"\n'
        'dataTransformationDescription: "## Step one\\n\\nWhat the step does.\\n"\n'
        'dataTransformationChangelog: "## 2026-01-01\\n\\n### Added\\n\\n'
        '- Dataset added to the graph.\\n"'
    )


@pytest.mark.parametrize("missing", DOCUMENTATION_FILES.values())
def test_export_bundle_fails_before_writing_when_a_documentation_file_is_missing(
    tmp_path: Path, missing: str
) -> None:
    """Test a missing documentation file fails the export, naming it, and writes nothing"""
    docs_dir = tmp_path / "docs"
    shutil.copytree(FIXTURES / "docs", docs_dir)
    (docs_dir / missing).unlink()
    out_dir = tmp_path / "bundle"

    with pytest.raises(FileNotFoundError, match=missing):
        export_bundle(
            dataclasses.replace(EXPORT_SPEC, docs_dir=docs_dir),
            connection=connect(tmp_path),
            out_dir=UPath(out_dir),
            log=LOG,
        )

    assert not any(out_dir.iterdir())


def spec_with_documentation(tmp_path: Path, name: str, texts: list[str]) -> ExportSpec:
    """The fixture spec over a docs directory holding `texts`, one per file"""
    docs_dir = tmp_path / name
    docs_dir.mkdir()
    for file_name, text in zip(DOCUMENTATION_FILES.values(), texts, strict=True):
        (docs_dir / file_name).write_text(text)
    return replace(EXPORT_SPEC, docs_dir=docs_dir)


def documentation_texts() -> list[str]:
    """One distinct text per documentation file"""
    return [f"text {index}" for index in range(len(DOCUMENTATION_FILES))]


# Each edit as the documentation texts before and after it.
DOCUMENTATION_EDITS = {
    **{
        f"{file_name} changed": (
            documentation_texts(),
            [
                f"{text} changed" if index == changed else text
                for index, text in enumerate(documentation_texts())
            ],
        )
        for changed, file_name in enumerate(DOCUMENTATION_FILES.values())
    },
    "text moved between files": (
        ["ab", "c", *documentation_texts()[2:]],
        ["a", "bc", *documentation_texts()[2:]],
    ),
}


@pytest.mark.parametrize(
    ("original", "edited"), DOCUMENTATION_EDITS.values(), ids=DOCUMENTATION_EDITS
)
def test_documentation_version_changes_with_each_documentation_edit(
    tmp_path: Path, original: list[str], edited: list[str]
) -> None:
    """Test an edit to any documentation file, or text moved between them, changes the version"""
    before = spec_with_documentation(tmp_path, "before", original)
    after = spec_with_documentation(tmp_path, "after", edited)

    assert after.documentation_version != before.documentation_version


def test_export_bundle_requires_exactly_one_provenance(tmp_path: Path) -> None:
    """Test the export fails when the provenance model holds two rows"""
    connection = connect(tmp_path)
    connection.execute(
        """
        insert into test_import.provenance
        select * from test_import.provenance
        """
    )

    with pytest.raises(ValueError):
        export_bundle(
            EXPORT_SPEC,
            connection=connection,
            out_dir=UPath(tmp_path / "bundle"),
            log=LOG,
        )


SOURCE_SQL = """
create or replace table test_import.source as
select
    'dc/s/Source1' as dcid,
    'Source One' as name,
    'https://example.org/' as url
"""


def test_export_bundle_writes_the_source_the_spec_names(tmp_path: Path) -> None:
    """Test the Source node takes its properties from the model's columns"""
    connection = connect(tmp_path)
    connection.execute(SOURCE_SQL)
    out_dir = UPath(tmp_path / "bundle")

    export_bundle(
        replace(EXPORT_SPEC, source_model="test_import.source"),
        connection=connection,
        out_dir=out_dir,
        log=LOG,
    )

    source_node = (Path(str(out_dir)) / "provenance.mcf").read_text().split("\n\n")[0]
    assert source_node == (
        "Node: dcid:dc/s/Source1\n"
        'name: "Source One"\n'
        "typeOf: dcid:Source\n"
        'url: "https://example.org/"'
    )


def test_export_bundle_requires_exactly_one_source(tmp_path: Path) -> None:
    """Test the export fails when the source model holds two rows"""
    connection = connect(tmp_path)
    connection.execute(SOURCE_SQL)
    connection.execute(
        """
        insert into test_import.source
        select * replace ('dc/s/Source2' as dcid) from test_import.source
        """
    )

    with pytest.raises(ValueError):
        export_bundle(
            replace(EXPORT_SPEC, source_model="test_import.source"),
            connection=connection,
            out_dir=UPath(tmp_path / "bundle"),
            log=LOG,
        )


def export_with_provenance_column(
    tmp_path: Path, *, column: str, value_sql: str, property_name: str
) -> Path:
    """Export the fixture bundle with one extra provenance column mapped to a property"""
    connection = connect(tmp_path)
    connection.execute(
        f"""
        create or replace table test_import.provenance as
        select *, {value_sql} as {column} from test_import.provenance
        """
    )
    out_dir = tmp_path / "bundle"
    export_bundle(
        replace(EXPORT_SPEC, provenance_column_to_property={column: property_name}),
        connection=connection,
        out_dir=UPath(out_dir),
        log=LOG,
    )
    return out_dir


def test_export_bundle_writes_the_conversion_recipe_onto_the_provenance(
    tmp_path: Path,
) -> None:
    """Test a spec's conversion recipe is written as a conversionRecipe reference arc"""
    out_dir = UPath(tmp_path / "bundle")

    export_bundle(
        replace(EXPORT_SPEC, conversion_recipe=ConversionRecipe.IMF),
        connection=connect(tmp_path),
        out_dir=out_dir,
        log=LOG,
    )

    provenance = (Path(str(out_dir)) / "provenance.mcf").read_text()
    assert "conversionRecipe: dcid:ConversionRecipeIMF\n" in provenance


def test_export_bundle_writes_no_conversion_recipe_when_the_spec_has_none(
    tmp_path: Path,
) -> None:
    """Test a spec without a conversion recipe writes no conversionRecipe arc"""
    _, out_dir = run_export(tmp_path)

    assert "conversionRecipe" not in (out_dir / "provenance.mcf").read_text()


def test_export_spec_requires_a_conversion_recipe() -> None:
    """Test a spec cannot be built without stating its conversion recipe"""
    fields = {
        spec_field.name: getattr(EXPORT_SPEC, spec_field.name)
        for spec_field in dataclasses.fields(EXPORT_SPEC)
        if spec_field.name != "conversion_recipe"
    }

    with pytest.raises(TypeError, match="conversion_recipe"):
        ExportSpec(**fields)


@pytest.mark.parametrize(
    ("value_sql", "expected"),
    [
        pytest.param("2024", "2024", id="integer"),
        pytest.param("'a \"b\"'", '"a \\u0022b\\u0022"', id="string"),
    ],
)
def test_export_bundle_writes_a_mapped_provenance_value(
    tmp_path: Path, value_sql: str, expected: str
) -> None:
    """Test a mapped integer is written as a number and a string as quoted, escaped text"""
    out_dir = export_with_provenance_column(
        tmp_path,
        column="deflator_base_year",
        value_sql=value_sql,
        property_name="deflatorBaseYear",
    )

    provenance = (out_dir / "provenance.mcf").read_text()
    assert f"deflatorBaseYear: {expected}\n" in provenance
    assert "deflator_base_year" not in provenance


@pytest.mark.parametrize(
    "value_sql",
    [
        pytest.param("date '2026-09-22'", id="date"),
        pytest.param("timestamp '2026-09-22 10:00:00'", id="timestamp"),
        pytest.param("null::varchar", id="null"),
        pytest.param("true", id="boolean"),
        pytest.param("2024.5::double", id="float"),
    ],
)
def test_export_bundle_refuses_a_mapped_provenance_value_mcf_cannot_carry(
    tmp_path: Path, value_sql: str
) -> None:
    """Test a mapped provenance value that is neither an integer nor a string fails the export, naming the column, and writes nothing"""
    with pytest.raises(ValueError, match="'deflator_base_year'"):
        export_with_provenance_column(
            tmp_path,
            column="deflator_base_year",
            value_sql=value_sql,
            property_name="deflatorBaseYear",
        )

    assert not any((tmp_path / "bundle").iterdir())


def test_export_bundle_refuses_a_mapped_column_the_provenance_lacks(
    tmp_path: Path,
) -> None:
    """Test a mapped column missing from the provenance model fails the export, naming it"""
    with pytest.raises(ValueError, match="deflator_base_year"):
        export_bundle(
            replace(
                EXPORT_SPEC,
                provenance_column_to_property={
                    "deflator_base_year": "deflatorBaseYear"
                },
            ),
            connection=connect(tmp_path),
            out_dir=UPath(tmp_path / "bundle"),
            log=LOG,
        )


@pytest.mark.parametrize(
    "reserved_property",
    [
        *DOCUMENTATION_FILES,
        "conversionRecipe",
        "lastDataRefreshDate",
    ],
)
def test_export_spec_refuses_mapping_onto_a_property_the_export_writes(
    reserved_property: str,
) -> None:
    """Test a provenance column cannot overwrite the conversion recipe or a property the docs directory supplies"""
    with pytest.raises(ValueError, match=reserved_property):
        replace(
            EXPORT_SPEC,
            provenance_column_to_property={"notes": reserved_property},
        )
