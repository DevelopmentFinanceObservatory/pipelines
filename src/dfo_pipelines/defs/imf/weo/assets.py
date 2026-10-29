import csv
import io
from datetime import UTC, date, datetime

import dagster as dg
import httpx
from dagster_duckdb import DuckDBResource
from imf_reader import weo
from imf_reader.weo import api as weo_api
from upath import UPath

from dfo_pipelines.data_commons.export import (
    DOCS_ROOT,
    ConversionRecipe,
    ExportSpec,
    ObservationInput,
    export_bundle,
)
from dfo_pipelines.data_commons.ingestion import ingest_bundle
from dfo_pipelines.defs.automation import (
    EAGER_ON_DEPS_OR_CODE_CHANGE_WHEN_CHECKS_PASS,
    EAGER_WHEN_CHECKS_PASS,
)
from dfo_pipelines.defs.dfo.ontology.assets import dfo_ontology_dc_ingestion
from dfo_pipelines.defs.dfo.shared.assets import dfo_shared_dc_ingestion
from dfo_pipelines.defs.resources import (
    KnowledgeGraphResource,
    UPathFileStoreResource,
)
from dfo_pipelines.extraction.metadata import (
    load_extraction_metadata,
    write_extraction_metadata,
)

# One observation of one series, with the dataflow's attributes, which include
# the date the IMF published the release.
PUBLICATION_DATE_URL = (
    "https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.RES/"
    "{dataflow_id}/{version}/USA.NGDP_RPCH.A?attributes=dataset&lastNObservations=1"
)

PUBLICATION_DATE_TIMEOUT_SECONDS = 30


@dg.observable_source_asset(
    group_name="imf_weo",
    automation_condition=dg.AutomationCondition.on_cron("@daily"),
)
def imf_weo_source() -> dg.ObserveResult:
    """Observed IMF WEO release.

    The source is observed daily via the IMF SDMX API so downstream assets
    only run when a new WEO release is published.
    """
    versions = weo.get_weo_versions()
    latest = versions[0]
    month, year = latest
    return dg.ObserveResult(
        metadata={"version": dg.MetadataValue.text(f"{month} {year}")},
        data_version=dg.DataVersion(f"{month};{year}"),
    )


@dg.asset(
    deps=[imf_weo_source],
    group_name="imf_weo",
    kinds={"file", "parquet"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    retry_policy=dg.RetryPolicy(max_retries=1),
    check_specs=[
        dg.AssetCheckSpec(
            name="file_not_empty", asset="imf_weo_raw_file", blocking=True
        )
    ],
)
def imf_weo_raw_file(
    context: dg.AssetExecutionContext, raw_file_store: UPathFileStoreResource
):
    """Raw IMF WEO bulk download file and metadata"""
    downloaded_at = datetime.now(UTC)
    directory = raw_file_store.build_date_partitioned_path("imf_weo/weo")
    directory.mkdir(parents=True, exist_ok=True)
    file_path = directory / "data.parquet"

    # 'version' is resolved and passed explicitly because with version=None,
    # imf_reader rolls back up to three releases when the newest one is listed
    # in the dataflow catalogue but not yet servable (reader.py:160). This would
    # return the older frame with no error — the gate would then hold a stale
    # release under the new release's data version until the next publication.
    version = weo.get_weo_versions()[0]
    dataframe = weo.fetch_data_with_metadata(version=version)
    dataframe.to_parquet(file_path, compression="gzip")
    write_extraction_metadata(
        directory,
        downloaded_at=downloaded_at,
        release_dates={"source": fetch_weo_publication_date(version)},
    )
    rows = dataframe.index.size
    file_size = file_path.stat().st_size
    context.log.info(f"Wrote IMF WEO data to {file_path}")
    return dg.MaterializeResult(
        value=str(file_path),
        metadata={
            "dagster/uri": dg.MetadataValue.text(str(file_path)),
            "file_size": dg.MetadataValue.int(file_size),
            "dagster/row_count": dg.MetadataValue.int(rows),
            "version": dg.MetadataValue.text(f"{version[0]} {version[1]}"),
        },
        check_results=[
            dg.AssetCheckResult(check_name="file_not_empty", passed=(rows > 0))
        ],
    )


@dg.asset(
    group_name="imf_weo",
    kinds={"duckdb"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    check_specs=[
        dg.AssetCheckSpec(
            name="table_not_empty", asset="imf_weo_raw_table", blocking=True
        )
    ],
)
def imf_weo_raw_table(
    duckdb: DuckDBResource, imf_weo_raw_file: str
) -> dg.MaterializeResult:
    """Raw IMF WEO database table"""
    schema = "imf_weo"
    table = "raw_weo"
    with duckdb.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(
            f"CREATE OR REPLACE TABLE {schema}.{table} AS SELECT * FROM read_parquet(?)",
            [imf_weo_raw_file],
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
    group_name="imf_weo",
    kinds={"duckdb"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
)
def imf_weo_raw_metadata(
    duckdb: DuckDBResource, imf_weo_raw_file: str
) -> dg.MaterializeResult:
    """Extraction metadata for the IMF WEO raw data"""
    schema = "imf_weo"
    table = "raw_weo_metadata"
    with duckdb.get_connection() as connection:
        rows = load_extraction_metadata(
            connection,
            directory=UPath(imf_weo_raw_file).parent,
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


# TODO: Use imf-reader's own lookup once it has a public one; this reads its
# private dataflow mapping: https://github.com/ONEcampaign/imf-reader/issues/44
def fetch_weo_publication_date(version: tuple[str, int]) -> date:
    """The date the IMF published a WEO release, from the dataflow serving it"""
    flow = weo_api._fetch_flow_mapping()[version]
    url = PUBLICATION_DATE_URL.format(
        dataflow_id=flow.dataflow_id, version=flow.version
    )
    with httpx.Client(timeout=PUBLICATION_DATE_TIMEOUT_SECONDS) as client:
        response = client.get(url, headers={"Accept": "text/csv"})
        response.raise_for_status()
    [observation] = csv.DictReader(io.StringIO(response.text))
    return datetime.fromisoformat(observation["PUBLICATION_DATE"]).date()


IMF_WEO_EXPORT_SPEC = ExportSpec(
    import_name="imf_weo",
    provenance_dcid="IMFWorldEconomicOutlook",
    provenance_model="imf_weo.dc_weo_provenance",
    conversion_recipe=ConversionRecipe.IMF,
    statvar_model="imf_weo.dc_weo_statvars",
    observation_inputs=(
        ObservationInput(
            model="imf_weo.dc_weo_observations",
            property_to_column={
                "variable": "variable",
                "date": "date",
                "value": "value",
                "unit": "unit",
                "measurementMethod": "measurement_method",
                "observationAbout": "observation_about",
            },
        ),
    ),
    source_model="imf_weo.weo_source",
    statvar_column_to_property={
        "name": "name",
        "source_indicator_code": "sourceIndicatorCode",
        "product_tax_basis": "productTaxBasis",
        "fixed_capital_consumption_basis": "fixedCapitalConsumptionBasis",
        "cyclical_adjustment": "cyclicalAdjustment",
        "currency_conversion_basis": "currencyConversionBasis",
        "final_expenditure_type": "finalExpenditureType",
    },
    docs_dir=DOCS_ROOT / "imf" / "weo",
    constant_observation_properties={"observationPeriod": "P1Y"},
)


@dg.asset(
    group_name="imf_weo",
    automation_condition=EAGER_ON_DEPS_OR_CODE_CHANGE_WHEN_CHECKS_PASS,
    code_version=IMF_WEO_EXPORT_SPEC.documentation_version,
    deps=[
        dg.AssetKey(["imf_weo", "dc_weo_observations"]),
        dg.AssetKey(["imf_weo", "dc_weo_provenance"]),
        dg.AssetKey(["imf_weo", "dc_weo_statvars"]),
        dg.AssetKey(["imf_weo", "weo_source"]),
    ],
)
def imf_weo_dc_export(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    export_file_store: UPathFileStoreResource,
) -> dg.MaterializeResult:
    """Data Commons export for IMF WEO"""
    out_dir = export_file_store.build_date_partitioned_path(
        IMF_WEO_EXPORT_SPEC.import_name
    )
    with duckdb.get_connection() as connection:
        export_metadata = export_bundle(
            IMF_WEO_EXPORT_SPEC, connection=connection, out_dir=out_dir, log=context.log
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
    group_name="imf_weo",
    kinds={"gcs"},
    deps=[dfo_ontology_dc_ingestion, dfo_shared_dc_ingestion],
)
def imf_weo_dc_ingestion(
    context: dg.AssetExecutionContext,
    knowledge_graph: KnowledgeGraphResource,
    imf_weo_dc_export: str,
) -> dg.MaterializeResult:
    """IMF WEO bundle ingested into the Data Commons knowledge graph

    Materialized by hand only: an ingestion rewrites the shared graph and runs
    for as long as it takes after the workflow is started.
    """
    destination = ingest_bundle(
        UPath(imf_weo_dc_export),
        import_name=IMF_WEO_EXPORT_SPEC.import_name,
        settings=knowledge_graph.settings,
        log=context.log,
    )
    return dg.MaterializeResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(destination),
            "import_name": dg.MetadataValue.text(IMF_WEO_EXPORT_SPEC.import_name),
        },
    )
