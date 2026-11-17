from datetime import date
from unittest.mock import patch

import httpx
import pytest

from dfo_pipelines.extraction.oecd import (
    fetch_bulk_file_release_date,
    resolve_bulk_file,
)

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
