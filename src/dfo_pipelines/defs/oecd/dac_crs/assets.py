import re
from datetime import UTC, date, datetime

import dagster as dg
import pyarrow.parquet as pq
from dagster_duckdb import DuckDBResource
from oda_reader import bulk_download_crs
from oda_reader.download.download_tools import (
    CRS_FLOW_URL,
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


def resolve_crs_bulk_file() -> tuple[str, str]:
    """Resolve the URL and version of the CRS bulk file.

    Returns:
        tuple[str, str]: The bulk file URL and its version token.

    Raises:
        ValueError: If the annotation carries no version.
    """
    # bypass_annotation_cache defeats oda_reader's 7-day on-disk HTTP cache,
    # which would otherwise let a daily observation serve a version token up to
    # a week old. Refreshing that cache entry is also what lets the
    # bulk_download_crs call in oecd_dac_crs_raw_file see the new version, since
    # it re-resolves the URL itself without a bypass.
    url, version = get_bulk_file_url_with_version(
        flow_url=CRS_FLOW_URL, label="CRS-Parquet", bypass_annotation_cache=True
    )
    if version is None:
        raise ValueError(f"CRS version check: could not get version for {url}")
    return url, version


def parse_crs_release_date(version: str) -> date:
    """Parse the date out of a version token.

    Args:
        version: Version token as oda_reader resolved it, of the form vYYYYMMDD.

    Returns:
        date: The parsed date.

    Raises:
        ValueError: If the token does not carry a date.
    """
    matched = re.fullmatch(r"v(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})", version)
    if matched is None:
        raise ValueError(
            f"CRS version token carries no release date, expected vYYYYMMDD: {version}"
        )
    return date(int(matched["year"]), int(matched["month"]), int(matched["day"]))


@dg.observable_source_asset(
    group_name="oecd_dac_crs",
    automation_condition=dg.AutomationCondition.on_cron("@daily"),
)
def oecd_dac_crs_source() -> dg.ObserveResult:
    """Observed OECD DAC CRS data version.

    The source is observed daily via calls to the OECD API.
    """
    url, version = resolve_crs_bulk_file()

    data_version = dg.DataVersion(";".join([url, version]))

    return dg.ObserveResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(url),
            "version": dg.MetadataValue.text(version),
        },
        data_version=data_version,
    )


@dg.asset(
    deps=[oecd_dac_crs_source],
    group_name="oecd_dac_crs",
    kinds={"file", "parquet"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    check_specs=[
        dg.AssetCheckSpec(
            name="file_not_empty", asset="oecd_dac_crs_raw_file", blocking=True
        )
    ],
)
def oecd_dac_crs_raw_file(
    context: dg.AssetExecutionContext, raw_file_store: UPathFileStoreResource
):
    """Raw OECD DAC CRS bulk download file and metadata"""
    downloaded_at = datetime.now(UTC)
    directory = raw_file_store.build_date_partitioned_path(
        "oecd_dac_crs/dac_crs", partition_date=downloaded_at.date()
    )
    directory.mkdir(parents=True, exist_ok=True)
    file_path = directory / "data.parquet"
    download_path = directory / "CRS.parquet"
    _, version = resolve_crs_bulk_file()
    bulk_download_crs(save_to_path=str(directory), use_raw_cache=False)
    download_path.rename(file_path)
    rows = pq.read_metadata(file_path).num_rows
    file_size = file_path.stat().st_size
    release_date = parse_crs_release_date(version)
    write_extraction_metadata(
        directory,
        downloaded_at=downloaded_at,
        release_dates={"source": release_date},
    )
    context.log.info(f"Wrote OECD DAC CRS data to {file_path}")
    return dg.MaterializeResult(
        value=str(file_path),
        metadata={
            "dagster/uri": dg.MetadataValue.text(str(file_path)),
            "file_size": dg.MetadataValue.int(file_size),
            "version": dg.MetadataValue.text(version),
            "dagster/row_count": dg.MetadataValue.int(rows),
            "source_release_date": dg.MetadataValue.text(release_date.isoformat()),
        },
        check_results=[
            dg.AssetCheckResult(check_name="file_not_empty", passed=(rows > 0))
        ],
    )


@dg.asset(
    group_name="oecd_dac_crs",
    kinds={"duckdb"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    check_specs=[
        dg.AssetCheckSpec(
            name="table_not_empty", asset="oecd_dac_crs_raw_table", blocking=True
        )
    ],
)
def oecd_dac_crs_raw_table(
    duckdb: DuckDBResource, oecd_dac_crs_raw_file: str
) -> dg.MaterializeResult:
    """Raw OECD DAC CRS database table"""
    schema = "oecd_dac_crs"
    table = "raw_dac_crs"
    with duckdb.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(
            f"CREATE OR REPLACE TABLE {schema}.{table} AS SELECT * FROM read_parquet(?)",
            [oecd_dac_crs_raw_file],
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
    group_name="oecd_dac_crs",
    kinds={"duckdb"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
)
def oecd_dac_crs_raw_metadata(
    duckdb: DuckDBResource, oecd_dac_crs_raw_file: str
) -> dg.MaterializeResult:
    """Extraction metadata for the OECD DAC CRS raw data"""
    schema = "oecd_dac_crs"
    table = "raw_dac_crs_metadata"
    with duckdb.get_connection() as connection:
        rows = load_extraction_metadata(
            connection,
            directory=UPath(oecd_dac_crs_raw_file).parent,
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


CRS_EXPORT_SPEC = ExportSpec(
    import_name="oecd_dac_crs",
    provenance_dcid="OECDCreditorReportingSystem",
    provenance_model="oecd_dac_crs.dc_dac_crs_provenance",
    conversion_recipe=ConversionRecipe.DAC,
    statvar_model="oecd_dac_crs.dc_dac_crs_statvars",
    observation_inputs=(
        ObservationInput(
            model="oecd_dac_crs.dc_dac_crs_observations",
            property_to_column={
                "variable": "variable",
                "date": "date",
                "value": "value",
                "unit": "unit",
                "custom:financingProvider": "financing_provider",
                "custom:financingRecipient": "financing_recipient",
            },
        ),
    ),
    statvar_column_to_property={
        "name": "name",
        "netting_basis": "nettingBasis",
        "financial_flow_type": "financialFlowType",
        "financial_obligation_type": "financialObligationType",
        "type_of_flow": "crsTypeOfFlow",
        "type_of_finance": "crsTypeOfFinance",
        "cooperation_modality": "crsCooperationModality",
        "purpose_code": "crsPurpose",
    },
    docs_dir=DOCS_ROOT / "oecd" / "dac_crs",
    constant_observation_properties={"observationPeriod": "P1Y"},
)


@dg.asset(
    group_name="oecd_dac_crs",
    automation_condition=EAGER_ON_DEPS_OR_CODE_CHANGE_WHEN_CHECKS_PASS,
    code_version=CRS_EXPORT_SPEC.documentation_version,
    deps=[
        dg.AssetKey(["oecd_dac_crs", "dc_dac_crs_observations"]),
        dg.AssetKey(["oecd_dac_crs", "dc_dac_crs_provenance"]),
        dg.AssetKey(["oecd_dac_crs", "dc_dac_crs_statvars"]),
    ],
)
def oecd_dac_crs_dc_export(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    export_file_store: UPathFileStoreResource,
) -> dg.MaterializeResult:
    """Data Commons export for OECD DAC CRS"""
    out_dir = export_file_store.build_date_partitioned_path(CRS_EXPORT_SPEC.import_name)
    with duckdb.get_connection() as connection:
        export_metadata = export_bundle(
            CRS_EXPORT_SPEC, connection=connection, out_dir=out_dir, log=context.log
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
    group_name="oecd_dac_crs",
    kinds={"gcs"},
    deps=[dfo_ontology_dc_ingestion, dfo_shared_dc_ingestion],
)
def oecd_dac_crs_dc_ingestion(
    context: dg.AssetExecutionContext,
    knowledge_graph: KnowledgeGraphResource,
    oecd_dac_crs_dc_export: str,
) -> dg.MaterializeResult:
    """OECD DAC CRS bundle ingested into the Data Commons knowledge graph

    Materialized by hand only: an ingestion rewrites the shared graph and runs
    for as long as it takes after the workflow is started.
    """
    destination = ingest_bundle(
        UPath(oecd_dac_crs_dc_export),
        import_name=CRS_EXPORT_SPEC.import_name,
        settings=knowledge_graph.settings,
        log=context.log,
    )
    return dg.MaterializeResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(destination),
            "import_name": dg.MetadataValue.text(CRS_EXPORT_SPEC.import_name),
        },
    )
