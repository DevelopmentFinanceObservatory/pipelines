from datetime import UTC, datetime

import dagster as dg
import pyarrow.parquet as pq
from dagster_duckdb import DuckDBResource
from oda_reader import bulk_download_dac1
from oda_reader.download.download_tools import (
    DAC1_FLOW_URL,
    get_bulk_file_url_with_version,
)
from upath import UPath

from dfo_pipelines.data_commons.export import (
    DOCS_ROOT,
    ConversionRecipe,
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
from dfo_pipelines.defs.world_bank.ids.assets import world_bank_ids_dc_ingestion
from dfo_pipelines.extraction.metadata import (
    load_extraction_metadata,
    write_extraction_metadata,
)
from dfo_pipelines.extraction.oecd import fetch_bulk_file_release_date


def resolve_dac1_bulk_file() -> tuple[str, str]:
    """The URL and version token of the DAC1 bulk file

    Raises:
        ValueError: If the lookup returns no version token.
    """
    # bypass_annotation_cache defeats oda_reader's 7-day on-disk HTTP cache,
    # which would otherwise let a daily observation serve a version token up to
    # a week old. Refreshing that cache entry is also what lets the
    # bulk_download_dac1 call in oecd_dac1_raw_file see the new version, since
    # it re-resolves the URL itself without a bypass.
    url, version = get_bulk_file_url_with_version(
        flow_url=DAC1_FLOW_URL,
        label="DAC1 full dataset (dotStat format)",
        bypass_annotation_cache=True,
    )
    if version is None:
        raise ValueError(f"DAC1 version check: could not get version for {url}")
    return url, version


@dg.observable_source_asset(
    group_name="oecd_dac1",
    automation_condition=dg.AutomationCondition.on_cron("@daily"),
)
def oecd_dac1_source() -> dg.ObserveResult:
    """Observed OECD DAC1 data version.

    The source is observed daily via calls to the OECD API.
    """
    url, version = resolve_dac1_bulk_file()

    data_version = dg.DataVersion(";".join([url, version]))

    return dg.ObserveResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(url),
            "version": dg.MetadataValue.text(version),
        },
        data_version=data_version,
    )


@dg.asset(
    deps=[oecd_dac1_source],
    group_name="oecd_dac1",
    kinds={"file", "parquet"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    check_specs=[
        dg.AssetCheckSpec(
            name="file_not_empty", asset="oecd_dac1_raw_file", blocking=True
        )
    ],
)
def oecd_dac1_raw_file(
    context: dg.AssetExecutionContext, raw_file_store: UPathFileStoreResource
):
    """Raw OECD DAC1 bulk download file and metadata"""
    downloaded_at = datetime.now(UTC)
    directory = raw_file_store.build_date_partitioned_path(
        "oecd_dac1/dac1", partition_date=downloaded_at.date()
    )
    directory.mkdir(parents=True, exist_ok=True)
    file_path = directory / "data.parquet"
    tmp_path = directory / "table1_data.parquet"
    url, version = resolve_dac1_bulk_file()
    release_date = fetch_bulk_file_release_date(url)
    bulk_download_dac1(save_to_path=str(directory), use_raw_cache=False)
    tmp_path.rename(file_path)
    rows = pq.read_metadata(file_path).num_rows
    file_size = file_path.stat().st_size
    write_extraction_metadata(
        directory, downloaded_at=downloaded_at, release_dates={"source": release_date}
    )
    context.log.info(f"Wrote OECD DAC1 data to {file_path}")
    return dg.MaterializeResult(
        value=str(file_path),
        metadata={
            "dagster/uri": dg.MetadataValue.text(str(file_path)),
            "file_size": dg.MetadataValue.int(file_size),
            "version": dg.MetadataValue.text(version),
            "dagster/row_count": dg.MetadataValue.int(rows),
        },
        check_results=[
            dg.AssetCheckResult(check_name="file_not_empty", passed=(rows > 0))
        ],
    )


@dg.asset(
    group_name="oecd_dac1",
    kinds={"duckdb"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    check_specs=[
        dg.AssetCheckSpec(
            name="table_not_empty", asset="oecd_dac1_raw_table", blocking=True
        )
    ],
)
def oecd_dac1_raw_table(
    duckdb: DuckDBResource, oecd_dac1_raw_file: str
) -> dg.MaterializeResult:
    """Raw OECD DAC1 database table"""
    schema = "oecd_dac1"
    table = "raw_dac1"
    published = pq.read_schema(oecd_dac1_raw_file).names
    renames = {
        "DONOR": "donor_code",
        "Donor": "donor_name",
        "PART": "part_code",
        "Part": "part_name",
    }
    aliases = [renames.get(name, name) for name in published]
    columns = ", ".join('"' + alias.replace('"', '""') + '"' for alias in aliases)
    with duckdb.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(
            f"CREATE OR REPLACE TABLE {schema}.{table} AS "
            f"SELECT * FROM read_parquet(?) AS source({columns})",
            [oecd_dac1_raw_file],
        )
        rows = (conn.sql(f"SELECT COUNT(*) FROM {schema}.{table}").fetchall())[0][0]
    return dg.MaterializeResult(
        metadata={
            "dagster/table_name": dg.MetadataValue.text(f"{schema}.{table}"),
            "dagster/row_count": dg.MetadataValue.int(rows),
        },
        check_results=[
            dg.AssetCheckResult(check_name="table_not_empty", passed=(rows > 0))
        ],
    )


@dg.asset(
    group_name="oecd_dac1",
    kinds={"duckdb"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
)
def oecd_dac1_raw_metadata(
    duckdb: DuckDBResource, oecd_dac1_raw_file: str
) -> dg.MaterializeResult:
    """Extraction metadata for the OECD DAC1 raw data"""
    schema = "oecd_dac1"
    table = "raw_dac1_metadata"
    with duckdb.get_connection() as connection:
        rows = load_extraction_metadata(
            connection,
            directory=UPath(oecd_dac1_raw_file).parent,
            schema=schema,
            table=table,
            release_names=("source",),
        )
    return dg.MaterializeResult(
        metadata={
            "dagster/table_name": dg.MetadataValue.text(f"{schema}.{table}"),
            "dagster/row_count": dg.MetadataValue.int(rows),
        },
    )


OECD_DAC1_EXPORT_SPEC = ExportSpec(
    import_name="oecd_dac1",
    provenance_dcid="OECDDAC1",
    provenance_model="oecd_dac1.dc_dac1_provenance",
    conversion_recipe=ConversionRecipe.DAC,
    statvar_model="oecd_dac1.dc_dac1_statvars",
    observation_inputs=(
        ObservationInput(
            model="oecd_dac1.dc_dac1_flow_observations",
            property_to_column={
                "variable": "variable",
                "date": "date",
                "value": "value",
                "unit": "unit",
                "custom:financingProvider": "financing_provider",
                "custom:financingRecipient": "financing_recipient",
            },
        ),
        ObservationInput(
            model="oecd_dac1.dc_dac1_donor_observations",
            property_to_column={
                "variable": "variable",
                "date": "date",
                "value": "value",
                "unit": "unit",
                "observationAbout": "observation_about",
            },
            file_stem="donor_observations",
        ),
    ),
    statvar_column_to_property={
        "name": "name",
        "financial_flow_type": "financialFlowType",
        "financial_obligation_type": "financialObligationType",
        "netting_basis": "nettingBasis",
        "fixed_capital_consumption_basis": "fixedCapitalConsumptionBasis",
        "dac_type_of_finance": "dacTypeOfFinance",
        "dac1_aid_type": "dac1AidType",
    },
    docs_dir=DOCS_ROOT / "oecd" / "dac1",
    constant_observation_properties={"observationPeriod": "P1Y"},
)


@dg.asset(
    group_name="oecd_dac1",
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    deps=[
        dg.AssetKey(["oecd_dac1", "dc_dac1_donor_observations"]),
        dg.AssetKey(["oecd_dac1", "dc_dac1_flow_observations"]),
        dg.AssetKey(["oecd_dac1", "dc_dac1_provenance"]),
        dg.AssetKey(["oecd_dac1", "dc_dac1_statvars"]),
    ],
)
def oecd_dac1_dc_export(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    export_file_store: UPathFileStoreResource,
) -> dg.MaterializeResult:
    """Data Commons export for OECD DAC1"""
    out_dir = export_file_store.build_date_partitioned_path(
        OECD_DAC1_EXPORT_SPEC.import_name
    )
    with duckdb.get_connection() as connection:
        export_metadata = export_bundle(
            OECD_DAC1_EXPORT_SPEC,
            connection=connection,
            out_dir=out_dir,
            log=context.log,
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


# TODO: Make this eager in a single-slot pool once the ingestion step can wait for
# the workflow to finish: https://github.com/ONEcampaign/dcp-tools/issues/109
@dg.asset(
    group_name="oecd_dac1",
    kinds={"gcs"},
    deps=[
        dfo_ontology_dc_ingestion,
        dfo_shared_dc_ingestion,
        # DAC1 publishes GNI under the StatVar the IDS import defines.
        world_bank_ids_dc_ingestion,
    ],
)
def oecd_dac1_dc_ingestion(
    context: dg.AssetExecutionContext,
    knowledge_graph: KnowledgeGraphResource,
    oecd_dac1_dc_export: str,
) -> dg.MaterializeResult:
    """OECD DAC1 bundle ingested into the Data Commons knowledge graph

    Materialized by hand only: an ingestion rewrites the shared graph and runs
    for as long as it takes after the workflow is started.
    """
    destination = ingest_bundle(
        UPath(oecd_dac1_dc_export),
        import_name=OECD_DAC1_EXPORT_SPEC.import_name,
        settings=knowledge_graph.settings,
        log=context.log,
    )
    return dg.MaterializeResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(destination),
        },
    )
