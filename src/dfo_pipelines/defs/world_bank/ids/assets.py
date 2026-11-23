"""World Bank International Debt Statistics (IDS) assets."""

from collections.abc import Mapping
from datetime import date

import dagster as dg
import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from upath import UPath

from dfo_pipelines.data_commons.export import (
    DOCS_ROOT,
    ConversionRecipe,
    ExportSpec,
    ObservationInput,
)
from dfo_pipelines.defs.dfo.ontology.assets import dfo_ontology_dc_ingestion
from dfo_pipelines.defs.dfo.shared.assets import dfo_shared_dc_ingestion
from dfo_pipelines.extraction.metadata import FetchedRelease
from dfo_pipelines.orchestration.dc_export import build_dc_export_asset
from dfo_pipelines.orchestration.dc_ingestion import build_dc_ingestion_asset
from dfo_pipelines.orchestration.landing import Observation, build_landing_asset
from dfo_pipelines.orchestration.raw_tables import RawTableSpec, build_raw_table_assets

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
            files[filename] = _bulk_file_metadata(url, response.headers)

    return files


def _bulk_file_metadata(
    url: str, headers: httpx.Headers
) -> dict[str, str | int | None]:
    """The metadata recorded for an IDS bulk file, from its response headers"""
    content_length = headers.get("content-length")
    return {
        "etag": headers.get("etag"),
        "last_modified": headers.get("last-modified"),
        "content_length": int(content_length) if content_length is not None else None,
        "version_id": headers.get("x-ms-version-id"),
        "url": url,
    }


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
        metadata={"files": dg.MetadataValue.json(files)},
        data_version=data_version,
    )


def fetch_ids(directory: UPath, observation: Observation) -> FetchedRelease:
    """Write the combined IDS bulk files the observation saw to `directory`

    An unmodified concatenation of the World Bank's six IDS bulk XLSX files
    ("Counterpart-Area: All") into a single wide-format parquet file.
    """
    source_release_date = fetch_ids_release_date()
    files = observation.metadata["files"]
    download_ids_bulk_data_files(directory, observed_files=files)
    combine_ids_bulk_data_files(directory, directory / "ids.parquet")
    return FetchedRelease(
        release_dates={"source": source_release_date}, metadata={"files": files}
    )


world_bank_ids_landing = build_landing_asset(
    group="world_bank_ids",
    source=world_bank_ids_source,
    dataset="ids",
    title="World Bank IDS",
    files=["ids.parquet"],
    fetch=fetch_ids,
)


world_bank_ids_raw_tables = build_raw_table_assets(
    group="world_bank_ids",
    title="World Bank IDS",
    tables=[
        RawTableSpec(
            asset="world_bank_ids_raw_table",
            file_stem="ids",
            table="raw_ids",
        )
    ],
    metadata_table="raw_ids_metadata",
)


def fetch_ids_release_date() -> date:
    """The date of the IDS release the bulk files hold, from the World Bank API"""
    with httpx.Client(timeout=OBSERVATION_TIMEOUT_SECONDS) as client:
        response = client.get(IDS_SOURCE_API_URL)
        response.raise_for_status()
    [_, [source]] = response.json()
    return date.fromisoformat(source["lastupdated"])


def download_ids_bulk_data_files(
    output_folder: UPath,
    *,
    observed_files: Mapping[str, dict[str, str | int | None]],
) -> None:
    """Download the six IDS bulk XLSX files into folder, as observed

    Args:
        output_folder: Folder the files are written to.
        observed_files: Each file's metadata when the source was observed.

    Raises:
        ValueError: If a file's version differs from the observed one.
    """
    try:
        with httpx.Client(follow_redirects=True, timeout=600) as client:
            for filename, url in BULK_XLSX_URLS.items():
                with client.stream("GET", url) as resp:
                    resp.raise_for_status()
                    observed = get_ids_bulk_download_version(
                        filename, observed_files[filename]
                    )
                    version = get_ids_bulk_download_version(
                        filename, _bulk_file_metadata(url, resp.headers)
                    )
                    if version != observed:
                        raise ValueError(
                            f"World Bank IDS bulk file {filename} moved from "
                            f"{observed} to {version} after it was observed: "
                            "observe it again"
                        )
                    with (output_folder / filename).open("wb") as f:
                        for chunk in resp.iter_bytes(chunk_size=DOWNLOAD_CHUNK_SIZE):
                            f.write(chunk)
    except Exception:
        for filename in BULK_XLSX_URLS:
            (output_folder / filename).unlink(missing_ok=True)
        raise


def combine_ids_bulk_data_files(input_folder: UPath, output_file: UPath) -> None:
    """Concatenate the downloaded XLSX files into a single parquet file.

    The files are combined unmodified (wide format as published, original
    column names, nulls and row order preserved) except that every value is
    carried through as text — see the dtype note below.

    Each file is appended as its own row group rather than being concatenated
    in memory first, so peak memory is bounded by the largest single file
    rather than by the whole dataset.
    """
    tmp_path = output_file.with_suffix(".tmp")
    writer: pq.ParquetWriter | None = None
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


world_bank_ids_dc_export = build_dc_export_asset(
    spec=IDS_EXPORT_SPEC,
    group="world_bank_ids",
    title="World Bank IDS",
)


world_bank_ids_dc_ingestion = build_dc_ingestion_asset(
    import_name=IDS_EXPORT_SPEC.import_name,
    group="world_bank_ids",
    title="World Bank IDS",
    deps=[dfo_ontology_dc_ingestion, dfo_shared_dc_ingestion],
)
