import json
from collections.abc import Callable, Iterable
from datetime import date
from pathlib import Path
from unittest.mock import ANY, patch

import dagster as dg
import httpx
import pandas as pd
import pytest
from dagster_duckdb import DuckDBResource
from upath import UPath

from dfo_pipelines.defs.world_bank.ids.assets import (
    BULK_XLSX_URLS,
    IDS_SOURCE_API_URL,
    download_ids_bulk_data_files,
    fetch_ids,
    fetch_ids_bulk_file_metadata,
    fetch_ids_release_date,
    get_ids_bulk_download_version,
    world_bank_ids_dc_export,
    world_bank_ids_source,
)
from dfo_pipelines.orchestration.landing import Observation
from dfo_pipelines.resources.file_store import UPathFileStoreResource
from tests.helpers import exported_directory

SOURCE_RELEASE_DATE = date(2025, 12, 3)


def patch_release_date(release_date: date = SOURCE_RELEASE_DATE):
    """Serve the World Bank API's IDS release date without a network call"""
    return patch(
        "dfo_pipelines.defs.world_bank.ids.assets.fetch_ids_release_date",
        return_value=release_date,
    )


# Each bulk file's metadata as its source observed it
OBSERVED_FILES: dict[str, dict[str, str | int | None]] = {
    filename: {"etag": f'"etag-{filename}-v1"'} for filename in BULK_XLSX_URLS
}

OBSERVATION = Observation(
    data_version="v1",
    metadata={
        "files": OBSERVED_FILES,
        "release_date": SOURCE_RELEASE_DATE.isoformat(),
    },
)


def _bulk_file_response(
    request: httpx.Request,
    *,
    version: str = "v1",
    content: Iterable[bytes] | None = None,
) -> httpx.Response:
    """A bulk file download at `version`, its body the request path by default"""
    filename = request.url.path.rsplit("/", 1)[1]
    return httpx.Response(
        200,
        headers={"etag": f'"etag-{filename}-{version}"'},
        content=f"payload for {request.url.path}".encode()
        if content is None
        else content,
    )


def _write_fake_downloads(folder: UPath, df: pd.DataFrame) -> None:
    """Stand-in for the download: write the frame as each of the six XLSX files."""
    for filename in BULK_XLSX_URLS:
        df.to_excel(folder / filename, index=False, engine="openpyxl")


def _fake_download(df: pd.DataFrame) -> Callable[..., None]:
    return lambda folder, observed_files: _write_fake_downloads(folder, df)


def _mock_http(handler: Callable[[httpx.Request], httpx.Response]):
    """Patch the module's httpx.Client so requests are served by handler."""
    real_client = httpx.Client

    def client(**kwargs):
        return real_client(**kwargs, transport=httpx.MockTransport(handler))

    return patch(
        "dfo_pipelines.defs.world_bank.ids.assets.httpx.Client", side_effect=client
    )


def _get_mock_head_headers(*, filename: str, version: str) -> dict[str, str]:
    """Get a set of mock HTTP headers for the data freshness checks

    Args:
        filename (str): the filename of the bulk XLSX file
        version (str): the version of the file, either "v1" or "v2"

    Returns:
        dict[str, str]: a dictionary of select HTTP headers for the HEAD request
            that are saved by Dagster
    """
    VERSION_HEADERS = {
        "v1": ("Thu, 14 May 2026 17:51:34 GMT", "2026-05-14T17:51:34.3247738Z"),
        "v2": ("Fri, 15 May 2026 17:51:34 GMT", "2026-05-15T17:51:34.3247738Z"),
    }

    return {
        "etag": f'"etag-{filename}-{version}"',
        "last-modified": VERSION_HEADERS[version][0],
        "content-length": "132758204",
        "x-ms-version-id": VERSION_HEADERS[version][1],
    }


def test_fetch_ids_bulk_file_metadata_heads_every_file() -> None:
    requested: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append((request.method, str(request.url)))
        return httpx.Response(
            200,
            headers=_get_mock_head_headers(
                filename=request.url.path.rsplit("/", 1)[1], version="v1"
            ),
        )

    with _mock_http(handler):
        result = fetch_ids_bulk_file_metadata()

    assert requested == [("HEAD", url) for url in BULK_XLSX_URLS.values()]
    assert result["A_D.xlsx"] == {
        "etag": '"etag-A_D.xlsx-v1"',
        "last_modified": "Thu, 14 May 2026 17:51:34 GMT",
        "content_length": 132758204,
        "version_id": "2026-05-14T17:51:34.3247738Z",
        "url": BULK_XLSX_URLS["A_D.xlsx"],
    }


def test_bulk_download_version_uses_etag_then_last_modified_then_version_id() -> None:
    assert (
        get_ids_bulk_download_version(
            "A_D.xlsx",
            {
                "etag": '"etag-a"',
                "last_modified": "Thu, 14 May 2026 17:51:34 GMT",
                "version_id": "2026-05-14T17:51:34.3247738Z",
            },
        )
        == "etag-a"
    )
    assert (
        get_ids_bulk_download_version(
            "A_D.xlsx",
            {
                "etag": None,
                "last_modified": "Thu, 14 May 2026 17:51:34 GMT",
                "version_id": "2026-05-14T17:51:34.3247738Z",
            },
        )
        == "Thu, 14 May 2026 17:51:34 GMT"
    )
    assert (
        get_ids_bulk_download_version(
            "A_D.xlsx",
            {
                "etag": None,
                "last_modified": None,
                "version_id": "2026-05-14T17:51:34.3247738Z",
            },
        )
        == "2026-05-14T17:51:34.3247738Z"
    )


def test_bulk_file_version_raises_when_headers_missing() -> None:
    with pytest.raises(
        ValueError,
        match="No version header for A_D\\.xlsx; cannot detect upstream changes",
    ):
        get_ids_bulk_download_version(
            "A_D.xlsx",
            {"etag": None, "last_modified": None, "version_id": None},
        )


def test_world_bank_ids_source_observe_fn_returns_metadata_and_data_version() -> None:
    observe = world_bank_ids_source.observe_fn
    assert observe is not None

    def handler(request: httpx.Request) -> httpx.Response:
        filename = request.url.path.rsplit("/", 1)[1]
        return httpx.Response(
            200, headers=_get_mock_head_headers(filename=filename, version="v1")
        )

    with _mock_http(handler), patch_release_date():
        result = observe()

    assert isinstance(result, dg.ObserveResult)
    assert result.data_version == dg.DataVersion(
        ";".join(
            [f"{filename}:etag-{filename}-v1" for filename in BULK_XLSX_URLS]
            + ["release_date:2025-12-03"]
        )
    )
    metadata = result.metadata
    assert metadata is not None
    assert metadata["release_date"] == dg.TextMetadataValue("2025-12-03")
    files = metadata["files"]
    assert isinstance(files, dg.JsonMetadataValue)
    assert files.data is not None
    assert files.data["A_D.xlsx"] == {
        "etag": '"etag-A_D.xlsx-v1"',
        "last_modified": "Thu, 14 May 2026 17:51:34 GMT",
        "content_length": 132758204,
        "version_id": "2026-05-14T17:51:34.3247738Z",
        "url": BULK_XLSX_URLS["A_D.xlsx"],
    }


def test_world_bank_ids_source_observe_fn_uses_last_modified_when_etag_missing() -> (
    None
):
    observe = world_bank_ids_source.observe_fn
    assert observe is not None

    def handler(request: httpx.Request) -> httpx.Response:
        filename = request.url.path.rsplit("/", 1)[1]
        headers = _get_mock_head_headers(filename=filename, version="v1")
        if filename == "A_D.xlsx":
            headers.pop("etag")
        return httpx.Response(200, headers=headers)

    with _mock_http(handler), patch_release_date():
        result = observe()

    assert result.data_version == dg.DataVersion(
        ";".join(
            ["A_D.xlsx:Thu, 14 May 2026 17:51:34 GMT"]
            + [
                f"{filename}:etag-{filename}-v1"
                for filename in list(BULK_XLSX_URLS)[1:]
            ]
            + ["release_date:2025-12-03"]
        )
    )


def test_world_bank_ids_source_observe_fn_raises_on_http_error() -> None:
    observe = world_bank_ids_source.observe_fn
    assert observe is not None

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    with _mock_http(handler), pytest.raises(httpx.HTTPStatusError):
        observe()


def test_world_bank_ids_source_observe_fn_raises_on_connection_error() -> None:
    observe = world_bank_ids_source.observe_fn
    assert observe is not None

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("timed out", request=request)

    with _mock_http(handler), pytest.raises(httpx.ConnectError):
        observe()


def test_fetch_ids_combines_the_downloads_unmodified(
    landing_directory: UPath,
) -> None:
    """Test the hook writes the six downloads as one text parquet file and returns
    the release date and the files' metadata"""
    file_path = landing_directory / "ids.parquet"
    source_df = pd.DataFrame(
        {
            "Country Code": ["AFG", "ZMB"],
            "Country Name": ["Afghanistan", "Zambia"],
            "Counterpart-Area Name": ["World", "Belgium"],
            # "071" has a leading zero: if the asset lets pandas infer types it
            # is read as the integer 71, corrupting an identifier later stages
            # join on. Keep a leading-zero code here so that regression fails.
            "Counterpart-Area Code": ["WLD", "071"],
            "Series Name": ["Debt stock, bilateral", "Debt stock, bilateral"],
            "Series Code": ["DT.DOD.BLAT.CD", "DT.DOD.BLAT.CD"],
            2023: [1.5, 2.5],
            2024: [3.0, 4.0],
        }
    )
    with patch(
        "dfo_pipelines.defs.world_bank.ids.assets.download_ids_bulk_data_files",
        side_effect=_fake_download(source_df),
    ) as download:
        release_dates, metadata = fetch_ids(landing_directory, OBSERVATION)

    download.assert_called_once_with(ANY, observed_files=OBSERVED_FILES)
    assert release_dates == {"source": SOURCE_RELEASE_DATE}
    assert metadata == {"files": OBSERVED_FILES}

    # files are combined unmodified: 2 rows x 6 files, original columns, year
    # headers carried through as strings
    with file_path.open("rb") as handle:
        out = pd.read_parquet(handle)
    assert len(out) == 12
    assert list(out.columns) == [
        "Country Code",
        "Country Name",
        "Counterpart-Area Name",
        "Counterpart-Area Code",
        "Series Name",
        "Series Code",
        "2023",
        "2024",
    ]
    # every value carried through as text, leading zeros intact
    assert set(out.dtypes.astype(str)) == {"str"}
    assert out["Counterpart-Area Code"].tolist() == ["WLD", "071"] * 6

    # the downloaded XLSX stay out of the landing directory
    assert [path.name for path in landing_directory.iterdir()] == ["ids.parquet"]


def test_fetch_ids_records_the_observed_release_date(tmp_path: Path) -> None:
    """Test the release date recorded is the one observed, not one the API moved
    to since"""
    with (
        patch_release_date(date(2026, 1, 15)),
        patch(
            "dfo_pipelines.defs.world_bank.ids.assets.download_ids_bulk_data_files",
            side_effect=_fake_download(pd.DataFrame({"Country Code": ["AFG"]})),
        ),
    ):
        release_dates, _ = fetch_ids(UPath(tmp_path), OBSERVATION)

    assert release_dates == {"source": SOURCE_RELEASE_DATE}


@pytest.mark.parametrize("read_to_fail_on", [1, 2])
def test_fetch_ids_cleans_up_when_combine_fails(
    landing_directory: UPath, read_to_fail_on: int
) -> None:
    """Test the output is removed when combining fails

    Parameterized with the read number to fail on so that a failure after the
    combined output file has been created can be tested.
    """
    file_path = landing_directory / "ids.parquet"
    df = pd.DataFrame({"Country Code": ["AFG"]})
    # Capture before patching: the patch target is the pandas module attribute
    # itself, so calling pd.read_excel inside the side effect would recurse.
    real_read_excel = pd.read_excel
    reads = 0

    def read_excel_fail_on_nth(*args, **kwargs) -> pd.DataFrame:
        nonlocal reads
        reads += 1
        if reads == read_to_fail_on:
            raise ValueError("corrupt workbook")
        return real_read_excel(*args, **kwargs)

    with (
        patch(
            "dfo_pipelines.defs.world_bank.ids.assets.download_ids_bulk_data_files",
            side_effect=_fake_download(df),
        ),
        patch(
            "dfo_pipelines.defs.world_bank.ids.assets.pd.read_excel",
            side_effect=read_excel_fail_on_nth,
        ),
        pytest.raises(ValueError, match="corrupt workbook"),
    ):
        fetch_ids(landing_directory, OBSERVATION)

    assert not file_path.exists()


def test_download_ids_bulk_data_files_writes_every_file(tmp_path: Path) -> None:
    """Test the download requests each bulk URL and streams it to disk"""
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        return _bulk_file_response(request)

    with _mock_http(handler):
        download_ids_bulk_data_files(UPath(tmp_path), observed_files=OBSERVED_FILES)

    assert requested == list(BULK_XLSX_URLS.values())
    for filename, url in BULK_XLSX_URLS.items():
        written = (tmp_path / filename).read_bytes()
        assert written == f"payload for {httpx.URL(url).path}".encode()


def test_download_ids_bulk_data_files_raises_on_http_error(tmp_path: Path) -> None:
    """Test an HTTP error on any bulk file surfaces rather than being skipped"""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    with _mock_http(handler), pytest.raises(httpx.HTTPStatusError):
        download_ids_bulk_data_files(UPath(tmp_path), observed_files=OBSERVED_FILES)


def test_download_ids_bulk_data_files_cleans_up_on_http_failure(tmp_path: Path) -> None:
    """Test already downloaded files are cleaned up on HTTP failure"""

    download_number: int = 0
    download_to_fail_on: int = 3

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal download_number
        download_number += 1
        if download_number == download_to_fail_on:
            return httpx.Response(404)
        return _bulk_file_response(request)

    with _mock_http(handler), pytest.raises(httpx.HTTPStatusError):
        download_ids_bulk_data_files(UPath(tmp_path), observed_files=OBSERVED_FILES)

    for filename in BULK_XLSX_URLS:
        assert not (tmp_path / filename).exists(), (
            f"Download not cleared up: {filename}"
        )


@pytest.mark.parametrize("download_to_fail_on", [1, 3, 6])
def test_download_ids_bulk_data_files_cleans_up_on_interrupted_download(
    tmp_path: Path, download_to_fail_on: int
) -> None:
    """Test any (partially) downloaded files are cleaned up on interrupted download"""

    download_number: int = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal download_number
        download_number += 1

        def send_chunk_then_fail():
            yield b"payload for "
            raise httpx.ReadError("connection reset")

        if download_number == download_to_fail_on:
            return _bulk_file_response(request, content=send_chunk_then_fail())

        return _bulk_file_response(request)

    with _mock_http(handler), pytest.raises(httpx.ReadError):
        download_ids_bulk_data_files(UPath(tmp_path), observed_files=OBSERVED_FILES)

    for filename in BULK_XLSX_URLS:
        assert not (tmp_path / filename).exists()


@pytest.mark.parametrize("download_that_moved", [1, 4])
def test_download_ids_bulk_data_files_refuses_a_file_that_moved_after_the_observation(
    tmp_path: Path, download_that_moved: int
) -> None:
    """Test a bulk file republished since its source was observed stops the download
    and leaves no file behind"""
    download_number = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal download_number
        download_number += 1
        version = "v2" if download_number == download_that_moved else "v1"
        return _bulk_file_response(request, version=version)

    with (
        _mock_http(handler),
        pytest.raises(ValueError, match="after it was observed: observe it again"),
    ):
        download_ids_bulk_data_files(UPath(tmp_path), observed_files=OBSERVED_FILES)

    assert list(tmp_path.iterdir()) == []


def test_fetch_ids_release_date_reads_the_source_record() -> None:
    """Test the release date is read from the API's lastupdated for IDS"""

    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == IDS_SOURCE_API_URL
        return httpx.Response(
            200,
            json=[
                {"page": "1", "pages": "1", "per_page": "50", "total": "1"},
                [{"id": "6", "lastupdated": "2025-12-03", "code": "IDS"}],
            ],
        )

    with _mock_http(handler):
        assert fetch_ids_release_date() == date(2025, 12, 3)


TOTAL_STATVAR = (
    "dcid:Amount_FinancialPosition_AmountOutstanding_LongTermMaturity_WithIMFCredit"
)
BREAKDOWN_STATVAR = "dcid:Amount_FinancialPosition_AmountOutstanding_ByCreditor"
GNI_STATVAR = "dcid:PrimaryIncome_EconomicUnit_GrossOfFixedCapitalConsumption_Nominal"

PROVENANCE_SQL = """
create or replace table world_bank_ids.dc_ids_provenance as
select
    'IDS Provenance' as name,
    'dc/s/TestSource' as source,
    'https://example.org/ids' as url,
    'https://example.org/ids/data.parquet' as source_data_url,
    'https://example.org/terms' as license,
    'dcid:TestLicenseType' as license_type,
    'P1Y' as source_release_frequency,
    '2026-09-22' as imported_source_release_date,
    '2026-12' as next_source_release_date,
    '2023' as earliest_observation_date,
    '2024' as latest_observation_date
"""

STATVARS_SQL = f"""
create or replace table world_bank_ids.dc_ids_statvars as
select * from (values
    (
        '{TOTAL_STATVAR}',
        'External debt outstanding, long-term',
        'External debt stocks, long-term + IMF (DOD)',
        'dcid:FinancialPosition',
        'dcid:amount',
        'dcid:measuredValue',
        null,
        null,
        'maturityBand,includesIMFCredit',
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        'dcid:LongTermMaturity',
        'dcid:WithIMFCredit',
        null,
        null,
        false,
        '"DT.DOD.DLTF.CD"',
        'financingRecipient'
    ),
    (
        '{BREAKDOWN_STATVAR}',
        'External debt outstanding, bilateral',
        'PPG, bilateral (DOD)',
        'dcid:FinancialPosition',
        'dcid:amount',
        'dcid:measuredValue',
        null,
        null,
        -- every remaining dimension at once, so each column-to-property binding
        -- the export performs is asserted below rather than a sample of them
        'financialObligationType,fixedCapitalConsumptionBasis,'
        'financialFlowType,nettingBasis,netOf,'
        'financialPositionType,currencyOfDenomination,financingResource,financingForm,'
        'idsConcessionality,idsCreditorType,guaranteeStatus,idsDebtorType,'
        'interestRateVariability',
        'dcid:Commitment',
        'dcid:GrossOfFixedCapitalConsumption',
        'dcid:Disbursement',
        'dcid:Net',
        'dcid:PrincipalRepayment',
        'dcid:AmountOutstanding',
        'dcid:CurrencyUSD',
        'dcid:DebtInstrument',
        'dcid:TechnicalCooperation',
        'dcid:IDSConcessional',
        'dcid:IDSCreditorTypeBLAT',
        'dcid:PublicOrPubliclyGuaranteed',
        null,
        null,
        'dcid:IDSDebtorTypeGG',
        'dcid:VariableInterestRate',
        true,
        '"DT.DOD.BLAT.CD"',
        'financingProvider,financingRecipient'
    ),
    (
        '{GNI_STATVAR}',
        'Gross national income',
        'GNI',
        'dcid:EconomicUnit',
        'dcid:primaryIncome',
        'dcid:measuredValue',
        'dcid:Nominal',
        null,
        'fixedCapitalConsumptionBasis',
        null,
        'dcid:GrossOfFixedCapitalConsumption',
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        false,
        '"NY.GNP.MKTP.CD"',
        null
    )
) as statvars (
    dcid,
    name,
    description,
    population_type,
    measured_property,
    stat_type,
    measurement_qualifier,
    measurement_denominator,
    constraint_properties,
    financial_obligation_type,
    fixed_capital_consumption_basis,
    financial_flow_type,
    netting_basis,
    net_of,
    financial_position_type,
    currency_of_denomination,
    financing_resource,
    financing_form,
    ids_concessionality,
    ids_creditor_type,
    guarantee_status,
    maturity_band,
    includes_imf_credit,
    ids_debtor_type,
    interest_rate_variability,
    has_provider,
    source_indicator_code,
    observation_properties
)
"""

OBSERVATIONS_SQL = f"""
create or replace table world_bank_ids.dc_ids_debt_observations as
select
    variable,
    date::integer as date,
    financing_provider,
    financing_recipient,
    value::decimal(18, 5) as value,
    unit,
    measurement_method
from (values
    ('{TOTAL_STATVAR}', 2023, null, 'country/AFG', 1.5, 'dcid:USDollars', null),
    ('{TOTAL_STATVAR}', 2024, null, 'country/AFG', 2.5, 'dcid:USDollars', 'dcid:Estimated'),
    ('{BREAKDOWN_STATVAR}', 2024, 'country/AUT', 'country/AFG', 3.5, 'dcid:Percent', null)
) as observations (
    variable, date, financing_provider, financing_recipient, value, unit, measurement_method
)
"""

ECONOMY_OBSERVATIONS_SQL = f"""
create or replace table world_bank_ids.dc_ids_economy_observations as
select
    variable,
    date::integer as date,
    observation_about,
    value::decimal(18, 5) as value,
    unit,
    measurement_method
from (values
    ('{GNI_STATVAR}', 2023, 'country/AFG', 20.5, 'dcid:CurrencyUSD', null)
) as observations (variable, date, observation_about, value, unit, measurement_method)
"""


def create_dc_models(duckdb: DuckDBResource) -> None:
    """Create the DC schema models the export reads, holding a small fixture bundle"""
    with duckdb.get_connection() as connection:
        connection.execute("create schema if not exists world_bank_ids")
        connection.execute(PROVENANCE_SQL)
        connection.execute(STATVARS_SQL)
        connection.execute(OBSERVATIONS_SQL)
        connection.execute(ECONOMY_OBSERVATIONS_SQL)


def run_export(tmp_path: Path) -> Path:
    """Materialize the export asset, returning the bundle directory"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "processed"))
    create_dc_models(duckdb)

    result = world_bank_ids_dc_export(
        context=dg.build_asset_context(),
        duckdb=duckdb,
        export_file_store=file_store,
    )
    return exported_directory(result)


def test_ids_dc_export_writes_each_observation_input_to_its_own_files_by_year(
    tmp_path: Path,
) -> None:
    """Test the bundle splits the flow and economy observations into files by year"""
    out_dir = run_export(tmp_path)

    assert sorted(path.name for path in out_dir.iterdir()) == [
        "config.json",
        "custom_nodes.mcf",
        "economy_observations_2023.csv",
        "observations_2023.csv",
        "observations_2024.csv",
        "provenance.mcf",
    ]


def test_ids_dc_export_registers_the_observation_columns(tmp_path: Path) -> None:
    """Test IDS export asset binds the observation columns to the properties DC reads"""
    out_dir = run_export(tmp_path)

    config = json.loads((out_dir / "config.json").read_text())

    assert config["importName"] == "world_bank_ids"
    assert [input_file["pattern"] for input_file in config["inputFiles"]] == [
        "*.mcf",
        "observations_*.csv",
        "economy_observations_*.csv",
    ]
    mcf, observations, economy_observations = config["inputFiles"]
    assert (
        mcf["provenance"]
        == observations["provenance"]
        == "dcid:provenance/WorldBankInternationalDebtStatistics"
    )
    assert observations["columnMappings"] == {
        "dcid:variableMeasured": "variable",
        "dcid:observationDate": "date",
        "dcid:value": "value",
        "dcid:unit": "unit",
        "dcid:measurementMethod": "measurement_method",
        "custom:financingProvider": "financing_provider",
        "custom:financingRecipient": "financing_recipient",
    }
    assert observations["observationProperties"] == {"observationPeriod": "P1Y"}
    assert economy_observations["provenance"] == mcf["provenance"]
    assert economy_observations["columnMappings"] == {
        "dcid:variableMeasured": "variable",
        "dcid:observationDate": "date",
        "dcid:value": "value",
        "dcid:unit": "unit",
        "dcid:measurementMethod": "measurement_method",
        "dcid:observationAbout": "observation_about",
    }

    header = (
        "variable,date,financing_provider,financing_recipient,value,unit,"
        "measurement_method"
    )
    assert (out_dir / "observations_2023.csv").read_text() == (
        f"{header}\n{TOTAL_STATVAR},2023,,country/AFG,1.50000,dcid:USDollars,\n"
    )
    lines = (out_dir / "observations_2024.csv").read_text().splitlines()
    assert lines[0] == header
    assert set(lines[1:]) == {
        f"{TOTAL_STATVAR},2024,,country/AFG,2.50000,dcid:USDollars,dcid:Estimated",
        f"{BREAKDOWN_STATVAR},2024,country/AUT,country/AFG,3.50000,dcid:Percent,",
    }


def test_ids_dc_export_writes_the_conversion_recipe_onto_the_provenance_node(
    tmp_path: Path,
) -> None:
    """Test the Provenance node carries the spec's conversion recipe as a conversionRecipe arc"""
    out_dir = run_export(tmp_path)

    provenance = (out_dir / "provenance.mcf").read_text()
    assert "conversionRecipe: dcid:ConversionRecipeIDS\n" in provenance


def test_ids_dc_export_maps_the_ids_dimensions_to_ontology_properties(
    tmp_path: Path,
) -> None:
    """Test IDS export asset names each dimension column as its ontology property"""
    out_dir = run_export(tmp_path)

    nodes = {
        node.splitlines()[0].removeprefix("Node: "): node.strip()
        for node in (out_dir / "custom_nodes.mcf").read_text().split("\n\n")
        if node.startswith("Node: ")
    }

    assert nodes[TOTAL_STATVAR] == (
        f"Node: {TOTAL_STATVAR}\n"
        'name: "External debt outstanding, long-term"\n'
        "typeOf: dcid:StatisticalVariable\n"
        'description: "External debt stocks, long-term + IMF (DOD)"\n'
        "statType: dcid:measuredValue\n"
        "populationType: dcid:FinancialPosition\n"
        "measuredProperty: dcid:amount\n"
        "constraintProperties: dcid:maturityBand, dcid:includesIMFCredit\n"
        "observationProperties: dcid:financingRecipient\n"
        "maturityBand: dcid:LongTermMaturity\n"
        "includesIMFCredit: dcid:WithIMFCredit\n"
        'sourceIndicatorCode: "DT.DOD.DLTF.CD"'
    )
    assert nodes[BREAKDOWN_STATVAR] == (
        f"Node: {BREAKDOWN_STATVAR}\n"
        'name: "External debt outstanding, bilateral"\n'
        "typeOf: dcid:StatisticalVariable\n"
        'description: "PPG, bilateral (DOD)"\n'
        "statType: dcid:measuredValue\n"
        "populationType: dcid:FinancialPosition\n"
        "measuredProperty: dcid:amount\n"
        "constraintProperties: "
        "dcid:financialObligationType, dcid:fixedCapitalConsumptionBasis, "
        "dcid:financialFlowType, dcid:nettingBasis, dcid:netOf, "
        "dcid:financialPositionType, dcid:currencyOfDenomination, "
        "dcid:financingResource, dcid:financingForm, dcid:idsConcessionality, "
        "dcid:idsCreditorType, dcid:guaranteeStatus, dcid:idsDebtorType, "
        "dcid:interestRateVariability\n"
        "observationProperties: dcid:financingProvider, dcid:financingRecipient\n"
        "financialObligationType: dcid:Commitment\n"
        "fixedCapitalConsumptionBasis: dcid:GrossOfFixedCapitalConsumption\n"
        "financialFlowType: dcid:Disbursement\n"
        "nettingBasis: dcid:Net\n"
        "netOf: dcid:PrincipalRepayment\n"
        "financialPositionType: dcid:AmountOutstanding\n"
        "currencyOfDenomination: dcid:CurrencyUSD\n"
        "financingResource: dcid:DebtInstrument\n"
        "financingForm: dcid:TechnicalCooperation\n"
        "idsConcessionality: dcid:IDSConcessional\n"
        "idsCreditorType: dcid:IDSCreditorTypeBLAT\n"
        "guaranteeStatus: dcid:PublicOrPubliclyGuaranteed\n"
        "idsDebtorType: dcid:IDSDebtorTypeGG\n"
        "interestRateVariability: dcid:VariableInterestRate\n"
        'sourceIndicatorCode: "DT.DOD.BLAT.CD"'
    )
    assert nodes[GNI_STATVAR] == (
        f"Node: {GNI_STATVAR}\n"
        'name: "Gross national income"\n'
        "typeOf: dcid:StatisticalVariable\n"
        'description: "GNI"\n'
        "statType: dcid:measuredValue\n"
        "populationType: dcid:EconomicUnit\n"
        "measuredProperty: dcid:primaryIncome\n"
        "constraintProperties: dcid:fixedCapitalConsumptionBasis\n"
        "measurementQualifier: dcid:Nominal\n"
        "fixedCapitalConsumptionBasis: dcid:GrossOfFixedCapitalConsumption\n"
        'sourceIndicatorCode: "NY.GNP.MKTP.CD"'
    )
