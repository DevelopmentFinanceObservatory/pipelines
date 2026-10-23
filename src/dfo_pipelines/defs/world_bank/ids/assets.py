"""World Bank International Debt Statistics (IDS) assets."""

from datetime import UTC, date, datetime

import dagster as dg
import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster_duckdb import DuckDBResource
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
from dfo_pipelines.extraction.metadata import (
    load_extraction_metadata,
    write_extraction_metadata,
)

BULK_XLSX_BASE_URL = "https://datacatalogfiles.worldbank.org/ddh-published/0038015"

# The six "Counterpart-Area: All" bulk XLSX files from World Bank Data Catalog
# dataset 0038015 (International Debt Statistics). Resource IDs are stable
# across releases.
BULK_XLSX_URLS: dict[str, str] = {
    "A_D.xlsx": f"{BULK_XLSX_BASE_URL}/DR0092201/A_D.xlsx",
    "E_K.xlsx": f"{BULK_XLSX_BASE_URL}/DR0092202/E_K.xlsx",
    "L_M.xlsx": f"{BULK_XLSX_BASE_URL}/DR0092203/L_M.xlsx",
    "N_Q.xlsx": f"{BULK_XLSX_BASE_URL}/DR0093225/N_Q.xlsx",
    "R_U.xlsx": f"{BULK_XLSX_BASE_URL}/DR0092204/R_U.xlsx",
    "V_Z.xlsx": f"{BULK_XLSX_BASE_URL}/DR0092205/V_Z.xlsx",
}

# The World Bank API's record of IDS (source 6), whose lastupdated field is the
# date of the release the bulk files hold.
IDS_SOURCE_API_URL = "https://api.worldbank.org/v2/sources/6?format=json"

DOWNLOAD_CHUNK_SIZE = 1 << 20  # 1 MB

OBSERVATION_TIMEOUT_SECONDS = 30


def fetch_ids_bulk_file_metadata() -> dict[str, dict[str, str | int | None]]:
    """Fetch the metadata for the IDS bulk data files using HEAD requests

    Returns:
        dict[str, dict[str, str | int | None]]: dictionary with one entry per
            URL (indexed on the filename part of the URL) which contains the
            HTTP headers that will be stored as the asset metadata
    """

    files: dict[str, dict[str, str | int | None]] = {}

    with httpx.Client(
        follow_redirects=True, timeout=OBSERVATION_TIMEOUT_SECONDS
    ) as client:
        for filename, url in BULK_XLSX_URLS.items():
            response = client.head(url)
            response.raise_for_status()
            content_length = response.headers.get("content-length")
            files[filename] = {
                "etag": response.headers.get("etag"),
                "last_modified": response.headers.get("last-modified"),
                "content_length": (
                    int(content_length) if content_length is not None else None
                ),
                "version_id": response.headers.get("x-ms-version-id"),
                "url": url,
            }

    return files


def get_ids_bulk_download_version(
    filename: str, file_metadata: dict[str, str | int | None]
) -> str:
    """Get a version string for the IDS bulk download file from its metadata.

    Args:
        filename (str): the filename of the bulk XLSX file
        file_metadata (dict[str, str  |  int  |  None]): the metadata dictionary
        for the file

    Raises:
        ValueError: if no version information is available in the metadata

    Returns:
        str: the version string for the file
    """
    etag = file_metadata.get("etag")
    if isinstance(etag, str) and etag:
        return etag.strip('"')

    last_modified = file_metadata.get("last_modified")
    if isinstance(last_modified, str) and last_modified:
        return last_modified

    version_id = file_metadata.get("version_id")
    if isinstance(version_id, str) and version_id:
        return version_id

    raise ValueError(
        f"No version header for {filename}; cannot detect upstream changes"
    )


@dg.observable_source_asset(
    group_name="world_bank_ids",
    automation_condition=dg.AutomationCondition.on_cron("@daily"),
)
def world_bank_ids_source() -> dg.ObserveResult:
    """Observed World Bank IDS bulk XLSX files.

    The source is observed daily via HTTP HEAD requests so downstream assets only
    run when the publisher has updated the files in place.
    """
    files = fetch_ids_bulk_file_metadata()
    data_version = dg.DataVersion(
        ";".join(
            f"{filename}:{get_ids_bulk_download_version(filename, file_metadata)}"
            for filename, file_metadata in files.items()
        )
    )

    return dg.ObserveResult(
        metadata={
            filename: dg.MetadataValue.json(file_metadata)
            for filename, file_metadata in files.items()
        },
        data_version=data_version,
    )


@dg.asset(
    deps=[world_bank_ids_source],
    group_name="world_bank_ids",
    kinds={"file", "parquet"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    check_specs=[
        dg.AssetCheckSpec(
            name="file_not_empty", asset="world_bank_ids_raw_file", blocking=True
        )
    ],
)
def world_bank_ids_raw_file(
    context: dg.AssetExecutionContext, raw_file_store: UPathFileStoreResource
):
    """Raw World Bank IDS bulk download file and metadata

    Unmodified concatenation of the World Bank's six IDS bulk XLSX files
    ("Counterpart-Area: All") into a single wide-format parquet file.
    """
    downloaded_at = datetime.now(UTC)
    directory = raw_file_store.build_date_partitioned_path("world_bank_ids/ids")
    directory.mkdir(parents=True, exist_ok=True)

    source_release_date = fetch_ids_release_date()
    download_ids_bulk_data_files(directory)

    output_file = directory / "data.parquet"
    rows = combine_ids_bulk_data_files(directory, output_file)
    write_extraction_metadata(
        directory,
        downloaded_at=downloaded_at,
        release_dates={"source": source_release_date},
    )

    file_size = output_file.stat().st_size

    context.log.info(
        f"Wrote World Bank IDS data to {output_file} ({file_size} bytes, {rows} rows)"
    )

    return dg.MaterializeResult(
        value=str(output_file),
        metadata={
            "dagster/uri": dg.MetadataValue.text(str(output_file)),
            "file_size": dg.MetadataValue.int(file_size),
        },
        check_results=[
            dg.AssetCheckResult(check_name="file_not_empty", passed=(rows > 0))
        ],
    )


@dg.asset(
    group_name="world_bank_ids",
    kinds={"duckdb"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    check_specs=[
        dg.AssetCheckSpec(
            name="table_not_empty", asset="world_bank_ids_raw_table", blocking=True
        )
    ],
)
def world_bank_ids_raw_table(
    duckdb: DuckDBResource, world_bank_ids_raw_file: str
) -> dg.MaterializeResult:
    """Raw World Bank IDS database table

    The combined bulk extract parquet, loaded unmodified (years in wide format,
    all values as text) into DuckDB. Fully overwritten on each materialization.
    """
    schema = "world_bank_ids"
    table = "raw_ids"
    with duckdb.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(
            f"CREATE OR REPLACE TABLE {schema}.{table} AS SELECT * FROM read_parquet(?)",
            [world_bank_ids_raw_file],
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
    group_name="world_bank_ids",
    kinds={"duckdb"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
)
def world_bank_ids_raw_metadata(
    duckdb: DuckDBResource, world_bank_ids_raw_file: str
) -> dg.MaterializeResult:
    """Extraction metadata for the World Bank IDS raw data"""
    schema = "world_bank_ids"
    table = "raw_ids_metadata"
    with duckdb.get_connection() as connection:
        rows = load_extraction_metadata(
            connection,
            directory=UPath(world_bank_ids_raw_file).parent,
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


def fetch_ids_release_date() -> date:
    """The date of the IDS release the bulk files hold, from the World Bank API"""
    with httpx.Client(timeout=OBSERVATION_TIMEOUT_SECONDS) as client:
        response = client.get(IDS_SOURCE_API_URL)
        response.raise_for_status()
    [_, [source]] = response.json()
    return date.fromisoformat(source["lastupdated"])


def download_ids_bulk_data_files(output_folder: UPath) -> None:
    """Download the six IDS bulk XLSX files into folder."""
    try:
        with httpx.Client(follow_redirects=True, timeout=600) as client:
            for filename, url in BULK_XLSX_URLS.items():
                with client.stream("GET", url) as resp:
                    resp.raise_for_status()
                    with (output_folder / filename).open("wb") as f:
                        for chunk in resp.iter_bytes(chunk_size=DOWNLOAD_CHUNK_SIZE):
                            f.write(chunk)
    except Exception:
        for filename in BULK_XLSX_URLS:
            (output_folder / filename).unlink(missing_ok=True)
        raise


def combine_ids_bulk_data_files(input_folder: UPath, output_file: UPath) -> int:
    """Concatenate the downloaded XLSX files into a single parquet file.

    The files are combined unmodified (wide format as published, original
    column names, nulls and row order preserved) except that every value is
    carried through as text — see the dtype note below.

    Each file is appended as its own row group rather than being concatenated
    in memory first, so peak memory is bounded by the largest single file
    rather than by the whole dataset. Returns the row count.
    """
    tmp_path = output_file.with_suffix(".tmp")
    writer: pq.ParquetWriter | None = None
    rows = 0

    try:
        try:
            for filename in BULK_XLSX_URLS:
                # Read every column as str because cleaning happens in later stages,
                # and letting pandas infer types is lossy: counterpart-area code
                # "071" would be read as the integer 71, silently corrupting an
                # identifier that later stages join on.
                df = pd.read_excel(
                    input_folder / filename, engine="openpyxl", dtype="str"
                )
                # The year headers arrive as ints, parquet requires string column names.
                # Make conversion explicit to avoid warnings on implicit conversion.
                df.columns = df.columns.map(str)

                table = pa.Table.from_pandas(df, preserve_index=False)
                del df

                if writer is None:
                    writer = pq.ParquetWriter(tmp_path, table.schema)

                # This raises if a file's schema differs from the first, rather than
                # silently null-filling the way pd.concat would.
                writer.write_table(table)
                rows += table.num_rows
                del table
        finally:
            if writer is not None:
                writer.close()
    except Exception:
        tmp_path.unlink(missing_ok=True)  # remove the partially written output file
        raise
    finally:
        for filename in BULK_XLSX_URLS:
            (input_folder / filename).unlink(missing_ok=True)

    tmp_path.rename(output_file)

    return rows


IDS_EXPORT_SPEC = ExportSpec(
    import_name="world_bank_ids",
    provenance_dcid="WorldBankInternationalDebtStatistics",
    provenance_model="world_bank_ids.dc_ids_provenance",
    conversion_recipe=ConversionRecipe.IDS,
    statvar_model="world_bank_ids.dc_ids_statvars",
    observation_inputs=(
        ObservationInput(
            model="world_bank_ids.dc_ids_debt_observations",
            property_to_column={
                "variable": "variable",
                "date": "date",
                "value": "value",
                "unit": "unit",
                "measurementMethod": "measurement_method",
                "custom:financingProvider": "financing_provider",
                "custom:financingRecipient": "financing_recipient",
            },
        ),
        ObservationInput(
            model="world_bank_ids.dc_ids_economy_observations",
            property_to_column={
                "variable": "variable",
                "date": "date",
                "value": "value",
                "unit": "unit",
                "measurementMethod": "measurement_method",
                "observationAbout": "observation_about",
            },
            file_stem="economy_observations",
        ),
    ),
    statvar_column_to_property={
        "name": "name",
        "description": "description",
        "source_indicator_code": "sourceIndicatorCode",
        "financial_obligation_type": "financialObligationType",
        "fixed_capital_consumption_basis": "fixedCapitalConsumptionBasis",
        "financial_flow_type": "financialFlowType",
        "netting_basis": "nettingBasis",
        "net_of": "netOf",
        "financial_position_type": "financialPositionType",
        "currency_of_denomination": "currencyOfDenomination",
        "financing_resource": "financingResource",
        "financing_form": "financingForm",
        "ids_concessionality": "idsConcessionality",
        "ids_creditor_type": "idsCreditorType",
        "guarantee_status": "guaranteeStatus",
        "maturity_band": "maturityBand",
        "includes_imf_credit": "includesIMFCredit",
        "ids_debtor_type": "idsDebtorType",
        "interest_rate_variability": "interestRateVariability",
    },
    statvar_ignore_columns=("has_provider",),
    statvar_csv_options={"dtype": str},
    docs_dir=DOCS_ROOT / "world_bank" / "ids",
    constant_observation_properties={"observationPeriod": "P1Y"},
)


@dg.asset(
    group_name="world_bank_ids",
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    deps=[
        dg.AssetKey(["world_bank_ids", "dc_ids_debt_observations"]),
        dg.AssetKey(["world_bank_ids", "dc_ids_economy_observations"]),
        dg.AssetKey(["world_bank_ids", "dc_ids_provenance"]),
        dg.AssetKey(["world_bank_ids", "dc_ids_statvars"]),
    ],
)
def world_bank_ids_dc_export(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    export_file_store: UPathFileStoreResource,
) -> dg.MaterializeResult:
    """Data Commons export for World Bank IDS"""
    out_dir = export_file_store.build_date_partitioned_path(IDS_EXPORT_SPEC.import_name)
    with duckdb.get_connection() as connection:
        export_metadata = export_bundle(
            IDS_EXPORT_SPEC, connection=connection, out_dir=out_dir, log=context.log
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
    group_name="world_bank_ids",
    kinds={"gcs"},
    deps=[dfo_ontology_dc_ingestion, dfo_shared_dc_ingestion],
)
def world_bank_ids_dc_ingestion(
    context: dg.AssetExecutionContext,
    knowledge_graph: KnowledgeGraphResource,
    world_bank_ids_dc_export: str,
) -> dg.MaterializeResult:
    """World Bank IDS bundle ingested into the Data Commons knowledge graph

    Materialized by hand only: an ingestion rewrites the shared graph and runs
    for as long as it takes after the workflow is started.
    """
    ingestion = ingest_bundle(
        UPath(world_bank_ids_dc_export),
        import_name=IDS_EXPORT_SPEC.import_name,
        settings=knowledge_graph.settings,
        log=context.log,
    )
    return dg.MaterializeResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(ingestion.destination),
            "import_name": dg.MetadataValue.text(IDS_EXPORT_SPEC.import_name),
            "execution_name": dg.MetadataValue.text(ingestion.execution_name),
        },
    )
