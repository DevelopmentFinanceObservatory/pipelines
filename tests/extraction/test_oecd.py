from datetime import date
from functools import partial
from pathlib import Path
from unittest.mock import Mock, call, patch

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from upath import UPath

from dfo_pipelines.extraction.oecd import (
    DAC_COLUMN_RENAMES,
    fetch_bulk_file,
    fetch_bulk_file_release_date,
    resolve_bulk_file,
)
from dfo_pipelines.warehouse import load_parquet

# Stand-in for a resolved bulk-download URL: never fetched, the GET is mocked.
BULK_FILE_URL = "https://webfs-dcd.oecd.org/data/dac1/DAC1.parquet"

FLOW_URL = "https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/DSD_DAC1@DF_DAC1"


def _release_date_response(headers: dict[str, str], status_code: int = 206):
    """Patch the ranged GET the release date lookup makes"""
    return patch(
        "dfo_pipelines.extraction.oecd.httpx.get",
        autospec=True,
        return_value=httpx.Response(
            status_code,
            headers=headers,
            request=httpx.Request("GET", BULK_FILE_URL),
        ),
    )


def test_fetch_bulk_file_release_date_reads_the_last_modified_date() -> None:
    """Test the release date is the bulk file's Last-Modified date, read by a ranged GET"""
    with _release_date_response(
        {"Last-Modified": "Tue, 07 Jul 2026 13:06:08 GMT"}
    ) as get:
        assert fetch_bulk_file_release_date(BULK_FILE_URL) == date(2026, 7, 7)

    assert get.call_args.args == (BULK_FILE_URL,)
    assert get.call_args.kwargs["headers"]["Range"] == "bytes=0-0"


def test_fetch_bulk_file_release_date_raises_without_last_modified() -> None:
    """Test a response with no Last-Modified header raises rather than guessing"""
    with (
        _release_date_response({}),
        pytest.raises(ValueError, match="no Last-Modified header"),
    ):
        fetch_bulk_file_release_date(BULK_FILE_URL)


def test_fetch_bulk_file_release_date_raises_on_http_error() -> None:
    """Test a refused request surfaces rather than recording a date"""
    with (
        _release_date_response({}, status_code=403),
        pytest.raises(httpx.HTTPStatusError),
    ):
        fetch_bulk_file_release_date(BULK_FILE_URL)


def _version_lookup(version: str | None):
    """Patch the bulk file lookup oda_reader makes"""
    return patch(
        "dfo_pipelines.extraction.oecd.get_bulk_file_url_with_version",
        autospec=True,
        return_value=(BULK_FILE_URL, version),
    )


def test_resolve_bulk_file_returns_the_url_and_version_bypassing_the_cache() -> None:
    """Test the bulk file is looked up by flow and label, bypassing oda_reader's
    annotation cache so a daily observation sees a republish"""
    with _version_lookup("0x8DEDC2882EAC20B") as lookup:
        assert resolve_bulk_file(flow_url=FLOW_URL, label="DAC1 full dataset") == (
            BULK_FILE_URL,
            "0x8DEDC2882EAC20B",
        )

    lookup.assert_called_once_with(
        flow_url=FLOW_URL, label="DAC1 full dataset", bypass_annotation_cache=True
    )


def test_resolve_bulk_file_raises_without_a_version() -> None:
    """Test a lookup with no version token raises, since nothing could gate a rerun"""
    with (
        _version_lookup(None),
        pytest.raises(ValueError, match="Bulk file version check"),
    ):
        resolve_bulk_file(flow_url=FLOW_URL, label="DAC1 full dataset")


LABEL = "Example full dataset"
DOWNLOADED_NAME = "example_data.parquet"

VERSION = "v20260801"

RELEASE_DATE = date(2026, 7, 7)


def write_download(save_to_path: str, use_raw_cache: bool) -> None:
    """Write a small table where oda_reader's bulk download writes its file"""
    pq.write_table(pa.table({"DONOR": [1, 2, 3]}), Path(save_to_path) / DOWNLOADED_NAME)


def build_fetch(*, download: Mock, release_date: Mock):
    return partial(
        fetch_bulk_file,
        flow_url=FLOW_URL,
        label=LABEL,
        observed_url=BULK_FILE_URL,
        observed_version=VERSION,
        download=download,
        downloaded_name=DOWNLOADED_NAME,
        release_date=release_date,
    )


def test_fetch_bulk_file_writes_the_download_as_published(tmp_path: Path) -> None:
    """Test the download is written to the path unchanged, dated by the
    release-date lookup, with the version the lookups before and after it agree on"""
    download = Mock(side_effect=write_download)
    release_date = Mock(return_value=RELEASE_DATE)
    fetch = build_fetch(download=download, release_date=release_date)

    with _version_lookup(VERSION) as lookup:
        release_dates, metadata = fetch(UPath(tmp_path / "data.parquet"))

    assert (
        lookup.call_args_list
        == [call(flow_url=FLOW_URL, label=LABEL, bypass_annotation_cache=True)] * 2
    )
    release_date.assert_called_once_with(BULK_FILE_URL, VERSION)
    download.assert_called_once_with(save_to_path=str(tmp_path), use_raw_cache=False)
    assert [path.name for path in tmp_path.iterdir()] == ["data.parquet"]
    assert pq.read_table(tmp_path / "data.parquet").to_pydict() == {"DONOR": [1, 2, 3]}
    assert release_dates == {"source": RELEASE_DATE}
    assert metadata == {"version": VERSION}


def test_fetch_bulk_file_resolves_the_bulk_file_before_downloading(
    tmp_path: Path,
) -> None:
    """Test the cache-bypassing lookup runs before the download, which re-resolves
    the URL through the cache that lookup refreshes"""
    with _version_lookup(VERSION) as lookup:

        def download(save_to_path: str, use_raw_cache: bool) -> None:
            lookup.assert_called_once()
            write_download(save_to_path, use_raw_cache)

        fetch = build_fetch(
            download=Mock(side_effect=download),
            release_date=Mock(return_value=RELEASE_DATE),
        )
        fetch(UPath(tmp_path / "data.parquet"))


def test_fetch_bulk_file_does_not_download_when_the_release_cannot_be_dated(
    tmp_path: Path,
) -> None:
    """Test a release-date lookup that fails stops the fetch before the download"""
    download = Mock(side_effect=write_download)
    fetch = build_fetch(
        download=download,
        release_date=Mock(side_effect=ValueError("no release date")),
    )

    with _version_lookup(VERSION), pytest.raises(ValueError, match="no release date"):
        fetch(UPath(tmp_path / "data.parquet"))

    download.assert_not_called()


@pytest.mark.parametrize(
    ("url", "version"),
    [
        (BULK_FILE_URL, "v20260901"),
        ("https://webfs-dcd.oecd.org/data/dac1/DAC1-2.parquet", VERSION),
    ],
)
def test_fetch_bulk_file_refuses_a_bulk_file_that_moved_after_the_observation(
    tmp_path: Path, url: str, version: str
) -> None:
    """Test a bulk file republished, or moved, since its source was observed stops
    the fetch before it is dated or downloaded"""
    download = Mock(side_effect=write_download)
    release_date = Mock(return_value=RELEASE_DATE)
    fetch = build_fetch(download=download, release_date=release_date)

    with (
        patch(
            "dfo_pipelines.extraction.oecd.get_bulk_file_url_with_version",
            autospec=True,
            return_value=(url, version),
        ),
        pytest.raises(ValueError, match="after it was observed: observe it again"),
    ):
        fetch(UPath(tmp_path / "data.parquet"))

    release_date.assert_not_called()
    download.assert_not_called()


def test_fetch_bulk_file_refuses_a_bulk_file_that_moved_during_the_download(
    tmp_path: Path,
) -> None:
    """Test a bulk file republished during the download is refused and its download
    deleted"""
    download = Mock(side_effect=write_download)
    fetch = build_fetch(download=download, release_date=Mock(return_value=RELEASE_DATE))

    with (
        patch(
            "dfo_pipelines.extraction.oecd.get_bulk_file_url_with_version",
            autospec=True,
            side_effect=[(BULK_FILE_URL, VERSION), (BULK_FILE_URL, "v20260901")],
        ),
        pytest.raises(ValueError, match="after it was observed: observe it again"),
    ):
        fetch(UPath(tmp_path / "data.parquet"))

    download.assert_called_once()
    assert list(tmp_path.iterdir()) == []


OECD_TESTS = Path(__file__).parents[1] / "defs" / "oecd"
DONOR_AND_PART_COLUMNS = {"donor_code", "donor_name", "part_code", "part_name"}
RECIPIENT_COLUMNS = {"recipient_code", "recipient_name"}


@pytest.mark.parametrize(
    ("dataset", "renamed"),
    [
        ("dac1", DONOR_AND_PART_COLUMNS),
        ("dac2a", DONOR_AND_PART_COLUMNS | RECIPIENT_COLUMNS),
        ("dac2b", DONOR_AND_PART_COLUMNS | RECIPIENT_COLUMNS),
    ],
)
def test_dac_column_renames_give_case_colliding_columns_distinct_names(
    dataset: str, renamed: set[str]
) -> None:
    """Test each case-colliding column pair of a published DAC file loads under
    distinct names"""
    path = OECD_TESTS / dataset / "fixtures" / "data.parquet"
    connection = duckdb.connect()

    load_parquet(
        connection,
        path=str(path),
        schema="oecd",
        table=dataset,
        renames=DAC_COLUMN_RENAMES,
    )

    assert renamed <= set(connection.sql(f"select * from oecd.{dataset}").columns)
