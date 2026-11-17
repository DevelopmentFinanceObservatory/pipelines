from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from functools import partial
from pathlib import Path
from unittest.mock import Mock, patch

import dagster as dg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from upath import UPath

from dfo_pipelines.orchestration.oecd_bulk_files import fetch_bulk_file

FLOW_URL = "https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/DSD_EXAMPLE"
LABEL = "Example full dataset"
DOWNLOADED_NAME = "example_data.parquet"

# Stand-in for the resolved bulk-download URL: never fetched, the lookup is mocked.
BULK_FILE_URL = "https://webfs-dcd.oecd.org/data/example/Example.parquet"
VERSION = "v20260801"

RELEASE_DATE = date(2026, 7, 7)


def write_download(save_to_path: str, use_raw_cache: bool) -> None:
    """Write a small table where oda_reader's bulk download writes its file"""
    pq.write_table(pa.table({"DONOR": [1, 2, 3]}), Path(save_to_path) / DOWNLOADED_NAME)


@contextmanager
def patch_bulk_lookup() -> Iterator[Mock]:
    """Answer the bulk file lookup the fetch makes"""
    with patch(
        "dfo_pipelines.extraction.oecd.get_bulk_file_url_with_version",
        autospec=True,
        return_value=(BULK_FILE_URL, VERSION),
    ) as lookup:
        yield lookup


def build_fetch(*, download: Mock, release_date: Mock):
    return partial(
        fetch_bulk_file,
        flow_url=FLOW_URL,
        label=LABEL,
        download=download,
        downloaded_name=DOWNLOADED_NAME,
        release_date=release_date,
    )


def test_fetch_bulk_file_writes_the_download_as_published(tmp_path: Path) -> None:
    """Test the downloaded file becomes the raw file unchanged, dated by the
    release-date lookup, with the version the download was resolved at"""
    download = Mock(side_effect=write_download)
    release_date = Mock(return_value=RELEASE_DATE)
    fetch = build_fetch(download=download, release_date=release_date)

    with patch_bulk_lookup() as lookup:
        release_dates, metadata = fetch(UPath(tmp_path / "data.parquet"))

    lookup.assert_called_once_with(
        flow_url=FLOW_URL, label=LABEL, bypass_annotation_cache=True
    )
    release_date.assert_called_once_with(BULK_FILE_URL, VERSION)
    download.assert_called_once_with(save_to_path=str(tmp_path), use_raw_cache=False)
    assert [path.name for path in tmp_path.iterdir()] == ["data.parquet"]
    assert pq.read_table(tmp_path / "data.parquet").to_pydict() == {"DONOR": [1, 2, 3]}
    assert release_dates == {"source": RELEASE_DATE}
    assert metadata == {"version": dg.TextMetadataValue(VERSION)}


def test_fetch_bulk_file_resolves_the_bulk_file_before_downloading(
    tmp_path: Path,
) -> None:
    """Test the cache-bypassing lookup runs before the download, which re-resolves
    the URL through the cache that lookup refreshes"""
    with patch_bulk_lookup() as lookup:

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

    with patch_bulk_lookup(), pytest.raises(ValueError, match="no release date"):
        fetch(UPath(tmp_path / "data.parquet"))

    download.assert_not_called()
