import json
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import patch

import dagster as dg
import httpx
import pandas as pd
import pytest
from dagster_duckdb import DuckDBResource
from upath import UPath

from dfo_pipelines.defs.resources import (
    KnowledgeGraphResource,
    UPathFileStoreResource,
)
from dfo_pipelines.defs.world_bank.ids.assets import (
    BULK_XLSX_URLS,
    IDS_EXPORT_SPEC,
    IDS_SOURCE_API_URL,
    download_ids_bulk_data_files,
    fetch_ids_bulk_file_metadata,
    fetch_ids_release_date,
    get_ids_bulk_download_version,
    world_bank_ids_dc_export,
    world_bank_ids_dc_ingestion,
    world_bank_ids_raw_file,
    world_bank_ids_raw_metadata,
    world_bank_ids_raw_table,
    world_bank_ids_source,
)
from dfo_pipelines.extraction.metadata import (
    read_extraction_metadata,
    write_extraction_metadata,
)
from tests.helpers import (
    AutomationHarness,
    column_list,
    list_first_row,
    meta,
    row_count,
)

FIXTURES = Path(__file__).parent / "fixtures"
FIXTURE_ROWS = len(pd.read_parquet(FIXTURES / "data.parquet"))


SOURCE_RELEASE_DATE = date(2025, 12, 3)
DOWNLOADED_AT = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)


def patch_release_date():
    """Serve the World Bank API's IDS release date without a network call"""
    return patch(
        "dfo_pipelines.defs.world_bank.ids.assets.fetch_ids_release_date",
        return_value=SOURCE_RELEASE_DATE,
    )


def _write_fake_downloads(folder: UPath, df: pd.DataFrame) -> None:
    """Stand-in for the download: write the frame as each of the six XLSX files."""
    for filename in BULK_XLSX_URLS:
        df.to_excel(folder / filename, index=False, engine="openpyxl")


def _fake_download(df: pd.DataFrame) -> Callable[[UPath], None]:
    return lambda folder: _write_fake_downloads(folder, df)


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


class _IDSAutomation(AutomationHarness):
    """Drives the IDS assets against an ephemeral instance, as the daemon does

    HEAD responses and downloads are mocked for as long as the context is open.
    Assign to `version` to make the next observation see a republished source.
    """

    def __init__(self, tmp_path: Path) -> None:
        super().__init__(
            tmp_path,
            source=world_bank_ids_source,
            raw_assets=[world_bank_ids_raw_file, world_bank_ids_raw_table],
        )
        self.version = "v1"

    def _start_mocks(self) -> None:
        # The gate compares data versions rather than data, so the frame's
        # contents just needs to not be empty.
        source_df = pd.DataFrame(
            {"Country Code": ["AFG"], "Country Name": ["Afghanistan"]}
        )
        self._stack.enter_context(_mock_http(self._head_response))
        self._stack.enter_context(patch_release_date())
        self._stack.enter_context(
            patch(
                "dfo_pipelines.defs.world_bank.ids.assets.download_ids_bulk_data_files",
                side_effect=_fake_download(source_df),
            )
        )

    def _head_response(self, request: httpx.Request) -> httpx.Response:
        """Answer a HEAD request with headers for the current version"""
        return httpx.Response(
            200,
            headers=_get_mock_head_headers(
                filename=request.url.path.rsplit("/", 1)[1], version=self.version
            ),
        )


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

    with _mock_http(handler):
        result = observe()

    assert isinstance(result, dg.ObserveResult)
    assert result.data_version == dg.DataVersion(
        ";".join(f"{filename}:etag-{filename}-v1" for filename in BULK_XLSX_URLS)
    )
    metadata = result.metadata
    assert metadata is not None
    file_metadata = metadata["A_D.xlsx"]
    assert isinstance(file_metadata, dg.JsonMetadataValue)
    assert file_metadata.data == {
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

    with _mock_http(handler):
        result = observe()

    assert result.data_version == dg.DataVersion(
        ";".join(
            ["A_D.xlsx:Thu, 14 May 2026 17:51:34 GMT"]
            + [
                f"{filename}:etag-{filename}-v1"
                for filename in list(BULK_XLSX_URLS)[1:]
            ]
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


def test_ids_source_observation_records_an_observation_event(tmp_path: Path) -> None:
    """Test observing the source records one observation against the source"""

    with _IDSAutomation(tmp_path) as automation:
        result = automation.observe()

    assert [
        entry.asset_key
        for entry in result.all_events
        if entry.event_type_value == "ASSET_OBSERVATION"
    ] == [world_bank_ids_source.key]


def test_ids_raw_assets_are_not_rerun_when_the_source_is_unchanged(
    tmp_path: Path,
) -> None:
    """Test a repeat observation at an unchanged data version requests no rerun

    This is the point of the new data check: the raw assets must not re-run
    while the World Bank has not republished the bulk files.
    """
    with _IDSAutomation(tmp_path) as automation:
        automation.observe()
        automation.materialize(world_bank_ids_raw_file, world_bank_ids_raw_table)
        # A baseline tick, not evidence: every condition is reset on its initial
        # evaluation, so the first tick requests nothing whatever the source did.
        automation.tick()

        automation.observe()

        assert automation.tick() == {
            "world_bank_ids_raw_file": 0,
            "world_bank_ids_raw_table": 0,
        }


def test_ids_source_is_requested_once_per_daily_cron_tick(tmp_path: Path) -> None:
    """Test the source's condition requests one observation per UTC day

    The 13:00 tick is what pins the cadence: a shorter cron would request an
    observation an hour after the last one. The 00:01 tick proves the condition
    requests an observation at all once the daily tick has passed, and 00:02
    proves it is not requested again before the next one.
    """
    with _IDSAutomation(tmp_path) as automation:
        # A baseline tick: every condition is reset on its initial evaluation,
        # so the first tick requests nothing whatever the time is.
        automation.source_tick(at=datetime(2026, 8, 25, 12, 0, tzinfo=UTC))

        assert automation.source_tick(at=datetime(2026, 8, 25, 13, 0, tzinfo=UTC)) == 0
        assert automation.source_tick(at=datetime(2026, 8, 26, 0, 1, tzinfo=UTC)) == 1
        assert automation.source_tick(at=datetime(2026, 8, 26, 0, 2, tzinfo=UTC)) == 0


def test_ids_raw_assets_are_rerun_when_the_source_data_version_changes(
    tmp_path: Path,
) -> None:
    """Test a changed data version requests the raw assets, and only once"""
    with _IDSAutomation(tmp_path) as automation:
        automation.observe()
        automation.materialize(world_bank_ids_raw_file, world_bank_ids_raw_table)
        automation.tick()  # baseline, as above

        automation.version = "v2"
        automation.observe()

        # Both are requested on the same tick: eager() counts a dependency that
        # will be requested as updated, so the chain is launched in one run.
        assert automation.tick() == {
            "world_bank_ids_raw_file": 1,
            "world_bank_ids_raw_table": 1,
        }

        automation.materialize(world_bank_ids_raw_file, world_bank_ids_raw_table)

        assert automation.tick() == {
            "world_bank_ids_raw_file": 0,
            "world_bank_ids_raw_table": 0,
        }


def test_ids_raw_file_writes_parquet(tmp_path: Path) -> None:
    """Test IDS raw file asset writes the download to a date-partitioned path"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
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
    with (
        patch_release_date(),
        patch("dfo_pipelines.defs.resources.date") as mock_date,
        patch(
            "dfo_pipelines.defs.world_bank.ids.assets.download_ids_bulk_data_files",
            side_effect=_fake_download(source_df),
        ) as download,
    ):
        mock_date.today.return_value = date(2026, 8, 3)
        raw_dir = file_store.build_date_partitioned_path("world_bank_ids/ids")
        result = world_bank_ids_raw_file(
            context=dg.build_asset_context(), raw_file_store=file_store
        )

    output_file = str(raw_dir / "data.parquet")

    download.assert_called_once()

    assert isinstance(result, dg.MaterializeResult)
    file_size = meta(result, "file_size", dg.IntMetadataValue)

    assert result.value == output_file
    assert meta(result, "dagster/uri", dg.TextMetadataValue).value == output_file
    assert file_size.value is not None and file_size.value > 0
    [check] = result.check_results
    assert check.check_name == "file_not_empty"
    assert check.passed

    # files are combined unmodified: 2 rows x 6 files, original columns, year
    # headers carried through as strings
    out = pd.read_parquet(output_file)
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

    # the downloaded XLSX are removed, leaving the published parquet and the
    # extraction metadata beside it
    assert sorted(p.name for p in raw_dir.iterdir()) == [
        "data.parquet",
        "metadata.json",
    ]
    recorded = read_extraction_metadata(raw_dir, release_names=("source",))
    assert recorded.release_dates == {"source": SOURCE_RELEASE_DATE}


def test_ids_raw_file_check_fails_when_no_rows(tmp_path: Path) -> None:
    """Test IDS raw file check fails when the combined download has no rows"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
    empty_df = pd.DataFrame({"Country Name": [], "Country Code": []})

    with (
        patch_release_date(),
        patch(
            "dfo_pipelines.defs.world_bank.ids.assets.download_ids_bulk_data_files",
            side_effect=_fake_download(empty_df),
        ),
    ):
        result = world_bank_ids_raw_file(
            context=dg.build_asset_context(), raw_file_store=file_store
        )

    assert isinstance(result, dg.MaterializeResult)

    [check] = result.check_results

    assert check.check_name == "file_not_empty"
    assert not check.passed


@pytest.mark.parametrize("read_to_fail_on", [1, 2])
def test_ids_raw_file_cleans_up_when_combine_fails(
    tmp_path: Path, read_to_fail_on: int
) -> None:
    """Test downloads and output are cleaned up when combining fails

    Parameterized with the read number to fail on so that a failure after the
    combined output file has been created can be tested.
    """
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
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
        patch_release_date(),
        patch("dfo_pipelines.defs.resources.date") as mock_date,
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
        mock_date.today.return_value = date(2026, 8, 3)
        raw_dir = file_store.build_date_partitioned_path("world_bank_ids/ids")
        world_bank_ids_raw_file(
            context=dg.build_asset_context(), raw_file_store=file_store
        )

    assert list(raw_dir.iterdir()) == []


def test_download_ids_bulk_data_files_writes_every_file(tmp_path: Path) -> None:
    """Test the download requests each bulk URL and streams it to disk"""
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        return httpx.Response(200, content=f"payload for {request.url.path}".encode())

    with _mock_http(handler):
        download_ids_bulk_data_files(UPath(tmp_path))

    assert requested == list(BULK_XLSX_URLS.values())
    for filename, url in BULK_XLSX_URLS.items():
        written = (tmp_path / filename).read_bytes()
        assert written == f"payload for {httpx.URL(url).path}".encode()


def test_download_ids_bulk_data_files_raises_on_http_error(tmp_path: Path) -> None:
    """Test an HTTP error on any bulk file surfaces rather than being skipped"""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    with _mock_http(handler), pytest.raises(httpx.HTTPStatusError):
        download_ids_bulk_data_files(UPath(tmp_path))


def test_download_ids_bulk_data_files_cleans_up_on_http_failure(tmp_path: Path) -> None:
    """Test already downloaded files are cleaned up on HTTP failure"""

    download_number: int = 0
    download_to_fail_on: int = 3

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal download_number
        download_number += 1
        if download_number == download_to_fail_on:
            return httpx.Response(404)
        return httpx.Response(200, content=f"payload for {request.url.path}".encode())

    with _mock_http(handler), pytest.raises(httpx.HTTPStatusError):
        download_ids_bulk_data_files(UPath(tmp_path))

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
            return httpx.Response(200, content=send_chunk_then_fail())

        return httpx.Response(200, content=f"payload for {request.url.path}".encode())

    with _mock_http(handler), pytest.raises(httpx.ReadError):
        download_ids_bulk_data_files(UPath(tmp_path))

    for filename in BULK_XLSX_URLS:
        assert not (tmp_path / filename).exists()


def test_ids_raw_table_loads_parquet(tmp_path: Path) -> None:
    """Test IDS raw table asset loads the parquet file into the database"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    result = world_bank_ids_raw_table(
        duckdb=duckdb, world_bank_ids_raw_file=str(FIXTURES / "data.parquet")
    )

    assert row_count(duckdb, "world_bank_ids.raw_ids") == FIXTURE_ROWS
    assert isinstance(result, dg.MaterializeResult)
    assert (
        meta(result, "dagster/table_name", dg.TextMetadataValue).value
        == "world_bank_ids.raw_ids"
    )
    assert meta(result, "dagster/row_count", dg.IntMetadataValue).value == FIXTURE_ROWS

    [check] = result.check_results
    assert check.check_name == "table_not_empty"
    assert check.passed

    # verify the load is unmodified: string year headers and the leading-zero
    # counterpart code survive intact
    with duckdb.get_connection() as conn:
        described = conn.sql("DESCRIBE world_bank_ids.raw_ids").fetchall()
        codes = conn.sql(
            'SELECT "Counterpart-Area Code" FROM world_bank_ids.raw_ids ORDER BY 1'
        ).fetchall()
    col_types = {name: dtype for name, dtype, *_ in described}
    assert col_types == {
        "Country Code": "VARCHAR",
        "Country Name": "VARCHAR",
        "Counterpart-Area Name": "VARCHAR",
        "Counterpart-Area Code": "VARCHAR",
        "Series Name": "VARCHAR",
        "Series Code": "VARCHAR",
        "2023": "VARCHAR",
        "2024": "VARCHAR",
        "2025": "VARCHAR",
    }
    assert [row[0] for row in codes] == sorted(
        pd.read_parquet(FIXTURES / "data.parquet")["Counterpart-Area Code"]
    )


def test_ids_raw_table_check_fails_when_table_empty(tmp_path: Path) -> None:
    """Test IDS raw table check fails when the parquet file has no rows"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    result = world_bank_ids_raw_table(
        duckdb=duckdb, world_bank_ids_raw_file=str(FIXTURES / "data-0.parquet")
    )

    assert row_count(duckdb, "world_bank_ids.raw_ids") == 0

    assert isinstance(result, dg.MaterializeResult)

    [check] = result.check_results

    assert check.check_name == "table_not_empty"
    assert not check.passed


def test_ids_raw_table_overwrites_existing_table(tmp_path: Path) -> None:
    """Test IDS raw table asset replaces the table rather than appending"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    world_bank_ids_raw_table(
        duckdb=duckdb, world_bank_ids_raw_file=str(FIXTURES / "data.parquet")
    )

    assert row_count(duckdb, "world_bank_ids.raw_ids") == FIXTURE_ROWS

    world_bank_ids_raw_table(
        duckdb=duckdb, world_bank_ids_raw_file=str(FIXTURES / "data-0.parquet")
    )

    assert row_count(duckdb, "world_bank_ids.raw_ids") == 0


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


def test_ids_raw_metadata_loads_the_recorded_metadata(tmp_path: Path) -> None:
    """Test the metadata asset loads the extraction metadata with the raw file"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    write_extraction_metadata(
        UPath(tmp_path),
        downloaded_at=DOWNLOADED_AT,
        release_dates={"source": SOURCE_RELEASE_DATE},
    )

    result = world_bank_ids_raw_metadata(
        duckdb=duckdb, world_bank_ids_raw_file=str(tmp_path / "data.parquet")
    )

    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata == {
        "dagster/table_name": dg.TextMetadataValue("world_bank_ids.raw_ids_metadata"),
        "dagster/row_count": dg.IntMetadataValue(1),
    }
    table = "world_bank_ids.raw_ids_metadata"
    assert column_list(duckdb, table) == ["downloaded_at", "source_release_date"]
    assert list_first_row(duckdb, table) == [
        DOWNLOADED_AT,
        pd.Timestamp(SOURCE_RELEASE_DATE),
    ]


def test_ids_raw_file_path_reaches_raw_table(tmp_path: Path) -> None:
    """Test the raw file asset's output path is injected into the raw table asset"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    source_df = pd.DataFrame(
        {
            "Country Code": ["AFG", "ZMB"],
            "Country Name": ["Afghanistan", "Zambia"],
            "Counterpart-Area Name": ["World", "Belgium"],
            "Counterpart-Area Code": ["WLD", "071"],
            "Series Name": ["Debt stock, bilateral", "Debt stock, bilateral"],
            "Series Code": ["DT.DOD.BLAT.CD", "DT.DOD.BLAT.CD"],
            2023: [1.5, 2.5],
            2024: [3.0, 4.0],
        }
    )

    with (
        patch_release_date(),
        patch(
            "dfo_pipelines.defs.world_bank.ids.assets.download_ids_bulk_data_files",
            side_effect=_fake_download(source_df),
        ),
    ):
        result = dg.materialize(
            [world_bank_ids_raw_file, world_bank_ids_raw_table],
            resources={"raw_file_store": file_store, "duckdb": duckdb},
        )

    checks = {e.check_name: e.passed for e in result.get_asset_check_evaluations()}

    assert checks == {"file_not_empty": True, "table_not_empty": True}
    assert row_count(duckdb, "world_bank_ids.raw_ids") == 12  # 2 rows x 6 files


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
    '2026-09-22' as source_release_date,
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


def run_export(tmp_path: Path) -> tuple[dg.MaterializeResult, Path]:
    """Materialize the export asset, returning the result and the bundle directory"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "processed"))
    create_dc_models(duckdb)

    with patch("dfo_pipelines.defs.resources.date") as mock_date:
        mock_date.today.return_value = date(2026, 8, 3)
        out_dir = file_store.build_date_partitioned_path("world_bank_ids")
        result = world_bank_ids_dc_export(
            context=dg.build_asset_context(),
            duckdb=duckdb,
            export_file_store=file_store,
        )

    assert isinstance(result, dg.MaterializeResult)

    return result, Path(str(out_dir))


def test_ids_dc_export_writes_a_bundle_to_a_date_partitioned_path(
    tmp_path: Path,
) -> None:
    """Test IDS export asset reports the bundle it wrote to today's partition"""
    result, out_dir = run_export(tmp_path)

    assert out_dir == tmp_path / "processed" / "world_bank_ids" / "2026" / "08" / "03"
    assert sorted(path.name for path in out_dir.iterdir()) == [
        "config.json",
        "custom_nodes.mcf",
        "economy_observations_2023.csv",
        "observations_2023.csv",
        "observations_2024.csv",
        "provenance.mcf",
    ]
    assert result.metadata == {
        "dagster/uri": dg.TextMetadataValue(str(out_dir)),
        "observation_count": dg.IntMetadataValue(4),
        "statvar_count": dg.IntMetadataValue(3),
        "observation_file_count": dg.IntMetadataValue(3),
    }


def test_ids_dc_export_registers_the_observation_columns(tmp_path: Path) -> None:
    """Test IDS export asset binds the observation columns to the properties DC reads"""
    _, out_dir = run_export(tmp_path)

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
    _, out_dir = run_export(tmp_path)

    provenance = (out_dir / "provenance.mcf").read_text()
    assert "conversionRecipe: dcid:ConversionRecipeIDS\n" in provenance


def test_ids_dc_export_maps_the_ids_dimensions_to_ontology_properties(
    tmp_path: Path,
) -> None:
    """Test IDS export asset names each dimension column as its ontology property"""
    _, out_dir = run_export(tmp_path)

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


def test_ids_ingestion_ingests_the_bundle_the_export_wrote(
    tmp_path: Path, knowledge_graph: KnowledgeGraphResource
) -> None:
    """Test the ingestion is handed the export's directory under its import name"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    create_dc_models(duckdb)
    import_name = IDS_EXPORT_SPEC.import_name
    destination = f"gs://test-bucket/ingestion/input/{import_name}"

    with patch(
        "dfo_pipelines.defs.world_bank.ids.assets.ingest_bundle",
        return_value=destination,
    ) as ingest_bundle:
        result = dg.materialize(
            [world_bank_ids_dc_export, world_bank_ids_dc_ingestion],
            resources={
                "duckdb": duckdb,
                "export_file_store": UPathFileStoreResource(
                    base_path=str(tmp_path / "processed")
                ),
                "knowledge_graph": knowledge_graph,
            },
        )

    assert result.success
    [export] = result.asset_materializations_for_node("world_bank_ids_dc_export")
    [ingestion] = result.asset_materializations_for_node("world_bank_ids_dc_ingestion")
    bundle = export.metadata["dagster/uri"].value
    ingest_bundle.assert_called_once()
    args, kwargs = ingest_bundle.call_args
    assert str(args[0]) == bundle
    assert kwargs["import_name"] == import_name
    assert kwargs["settings"] == knowledge_graph.settings
    assert ingestion.metadata["dagster/uri"].value == destination
