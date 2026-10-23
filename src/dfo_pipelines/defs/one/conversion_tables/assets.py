"""The ONE conversion tables: exchange rates and US-dollar deflators.

The tables are computed in dbt from DAC1 and the WEO, so this module holds only
the two exports and their ingestions. Each table is its own import under its own
provenance, because an ingest replaces everything carrying its import's provenance
and the two tables refresh with different sources.
"""

import dagster as dg
from dagster_duckdb import DuckDBResource
from upath import UPath

from dfo_pipelines.data_commons.export import (
    DOCS_ROOT,
    ExportSpec,
    ObservationInput,
    export_bundle,
)
from dfo_pipelines.data_commons.ingestion import ingest_bundle
from dfo_pipelines.defs.automation import EAGER_WHEN_CHECKS_PASS
from dfo_pipelines.defs.dfo.ontology.assets import dfo_ontology_dc_ingestion
from dfo_pipelines.defs.dfo.shared.assets import dfo_shared_dc_ingestion
from dfo_pipelines.defs.resources import (
    KnowledgeGraphResource,
    UPathFileStoreResource,
)

SCHEMA = "one_conversion_tables"
DOCS_DIR = DOCS_ROOT / "one" / "conversion_tables"


def _export_spec(*, table: str) -> ExportSpec:
    """The export of one conversion table, `dac` or `imf`."""
    return ExportSpec(
        import_name=f"one_conversion_tables_{table}",
        provenance_dcid=f"ONEConversionTables{table.upper()}",
        provenance_model=f"{SCHEMA}.dc_conversion_tables_{table}_provenance",
        conversion_recipe=None,
        statvar_model=f"{SCHEMA}.dc_conversion_tables_{table}_statvars",
        observation_inputs=(
            ObservationInput(
                model=f"{SCHEMA}.dc_conversion_tables_{table}_observations",
                property_to_column={
                    "variable": "variable",
                    "date": "date",
                    "value": "value",
                    "unit": "unit",
                    "observationAbout": "observation_about",
                    # Null on a reported value, so only projected rows carry it.
                    "measurementMethod": "measurement_method",
                },
            ),
        ),
        statvar_column_to_property={"name": "name"},
        docs_dir=DOCS_DIR,
        constant_observation_properties={"observationPeriod": "P1Y"},
        provenance_column_to_property={"deflator_base_year": "deflatorBaseYear"},
    )


DAC_EXPORT_SPEC = _export_spec(table="dac")
IMF_EXPORT_SPEC = _export_spec(table="imf")


# The rate diagnostics feed no publication model, so their blocking checks gate
# an export only through its dependencies. Each export waits on the diagnostics
# of every table it reads: the DAC table fills from the IMF table's rates, so the
# DAC export also waits on the IMF agreement check, and the IMF export, which
# reads no DAC1 model, waits on that check alone.
RATE_DIAGNOSTICS = {
    "dac": ("dac_rate_agreement", "dac_rate_band", "imf_rate_agreement"),
    "imf": ("imf_rate_agreement",),
}


def _dbt_model_keys(*, table: str) -> list[dg.AssetKey]:
    """The three dc models one table's export reads, and its rate diagnostics."""
    return [
        dg.AssetKey([SCHEMA, f"dc_conversion_tables_{table}_{model}"])
        for model in ("observations", "provenance", "statvars")
    ] + [
        dg.AssetKey([SCHEMA, f"int_conversion_tables_{diagnostic}"])
        for diagnostic in RATE_DIAGNOSTICS[table]
    ]


def _export(
    spec: ExportSpec,
    *,
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    export_file_store: UPathFileStoreResource,
) -> dg.MaterializeResult:
    """Write `spec`'s bundle to today's partition and report what it holds. The
    bundle's directory is the asset's value, which its ingestion reads."""
    out_dir = export_file_store.build_date_partitioned_path(spec.import_name)
    with duckdb.get_connection() as connection:
        export_metadata = export_bundle(
            spec, connection=connection, out_dir=out_dir, log=context.log
        )
    return dg.MaterializeResult(
        value=str(out_dir),
        metadata={
            "dagster/uri": dg.MetadataValue.text(str(out_dir)),
            "observation_count": dg.MetadataValue.int(
                export_metadata.observation_count
            ),
            "statvar_count": dg.MetadataValue.int(export_metadata.statvar_count),
            "observation_file_count": dg.MetadataValue.int(
                export_metadata.observation_file_count
            ),
        },
    )


@dg.asset(
    group_name="one_conversion_tables",
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    deps=_dbt_model_keys(table="dac"),
)
def one_conversion_tables_dac_dc_export(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    export_file_store: UPathFileStoreResource,
) -> dg.MaterializeResult:
    """Data Commons export for the DAC conversion table"""
    return _export(
        DAC_EXPORT_SPEC,
        context=context,
        duckdb=duckdb,
        export_file_store=export_file_store,
    )


@dg.asset(
    group_name="one_conversion_tables",
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    deps=_dbt_model_keys(table="imf"),
)
def one_conversion_tables_imf_dc_export(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    export_file_store: UPathFileStoreResource,
) -> dg.MaterializeResult:
    """Data Commons export for the IMF conversion table"""
    return _export(
        IMF_EXPORT_SPEC,
        context=context,
        duckdb=duckdb,
        export_file_store=export_file_store,
    )


def _ingest(
    spec: ExportSpec,
    *,
    context: dg.AssetExecutionContext,
    knowledge_graph: KnowledgeGraphResource,
    bundle: str,
) -> dg.MaterializeResult:
    """Ingest the bundle `spec`'s export wrote and report where it was staged."""
    ingestion = ingest_bundle(
        UPath(bundle),
        import_name=spec.import_name,
        settings=knowledge_graph.settings,
        log=context.log,
    )
    return dg.MaterializeResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(ingestion.destination),
            "import_name": dg.MetadataValue.text(spec.import_name),
            "execution_name": dg.MetadataValue.text(ingestion.execution_name),
        },
    )


# TODO: Make these eager in a single-slot pool once the ingestion step can wait for
# the workflow to finish: https://github.com/ONEcampaign/dcp-tools/issues/109
@dg.asset(
    group_name="one_conversion_tables",
    kinds={"gcs"},
    deps=[dfo_ontology_dc_ingestion, dfo_shared_dc_ingestion],
)
def one_conversion_tables_dac_dc_ingestion(
    context: dg.AssetExecutionContext,
    knowledge_graph: KnowledgeGraphResource,
    one_conversion_tables_dac_dc_export: str,
) -> dg.MaterializeResult:
    """DAC conversion table bundle ingested into the Data Commons knowledge graph

    Materialized by hand only: an ingestion rewrites the shared graph and runs
    for as long as it takes after the workflow is started.
    """
    return _ingest(
        DAC_EXPORT_SPEC,
        context=context,
        knowledge_graph=knowledge_graph,
        bundle=one_conversion_tables_dac_dc_export,
    )


@dg.asset(
    group_name="one_conversion_tables",
    kinds={"gcs"},
    deps=[dfo_ontology_dc_ingestion, dfo_shared_dc_ingestion],
)
def one_conversion_tables_imf_dc_ingestion(
    context: dg.AssetExecutionContext,
    knowledge_graph: KnowledgeGraphResource,
    one_conversion_tables_imf_dc_export: str,
) -> dg.MaterializeResult:
    """IMF conversion table bundle ingested into the Data Commons knowledge graph

    Materialized by hand only: an ingestion rewrites the shared graph and runs
    for as long as it takes after the workflow is started.
    """
    return _ingest(
        IMF_EXPORT_SPEC,
        context=context,
        knowledge_graph=knowledge_graph,
        bundle=one_conversion_tables_imf_dc_export,
    )
