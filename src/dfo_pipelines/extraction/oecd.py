from collections.abc import Callable
from datetime import date
from email.utils import parsedate_to_datetime

import httpx
from oda_reader.download.download_tools import (
    DEFAULT_HEADERS,
    get_bulk_file_url_with_version,
)
from upath import UPath

from dfo_pipelines.extraction.metadata import FetchedRelease

RELEASE_DATE_TIMEOUT_SECONDS = 30

# DAC parquet column pairs differ only in case, which DuckDB and BigQuery can't hold.
DAC_COLUMN_RENAMES = {
    "DONOR": "donor_code",
    "Donor": "donor_name",
    "RECIPIENT": "recipient_code",
    "Recipient": "recipient_name",
    "PART": "part_code",
    "Part": "part_name",
}


def resolve_bulk_file(*, flow_url: str, label: str) -> tuple[str, str]:
    """The URL and version token of the OECD bulk file `label` lists at `flow_url`

    Raises:
        ValueError: If the lookup returns no version token.
    """
    # bypass_annotation_cache defeats oda_reader's 7-day on-disk HTTP cache,
    # which would otherwise let a daily observation serve a version token up to
    # a week old.
    url, version = get_bulk_file_url_with_version(
        flow_url=flow_url, label=label, bypass_annotation_cache=True
    )
    if version is None:
        raise ValueError(
            f"Bulk file version check: could not get the version of {label!r} at {url}"
        )
    return url, version


def fetch_bulk_file_release_date(url: str) -> date:
    """The date OECD last published a bulk file, from its Last-Modified header

    The DAC1, DAC2a and DAC2b version tokens carry no date, unlike the CRS one.
    A one-byte ranged GET reads the header, since the host refuses HEAD requests.

    Raises:
        ValueError: If the response carries no Last-Modified header.
    """
    response = httpx.get(
        url,
        headers={**DEFAULT_HEADERS, "Range": "bytes=0-0"},
        timeout=RELEASE_DATE_TIMEOUT_SECONDS,
        follow_redirects=True,
    )
    response.raise_for_status()
    last_modified = response.headers.get("Last-Modified")
    if last_modified is None:
        raise ValueError(f"Bulk file release date: no Last-Modified header for {url}")
    return parsedate_to_datetime(last_modified).date()


def fetch_bulk_file(
    file_path: UPath,
    *,
    flow_url: str,
    label: str,
    observed_url: str,
    observed_version: str,
    download: Callable[..., object],
    downloaded_name: str,
    release_date: Callable[[str, str], date],
) -> FetchedRelease:
    """Download the bulk file `label` lists at `flow_url` to `file_path`, as observed

    Args:
        file_path: Path in the landing directory the bulk file is written to.
        flow_url: The OECD dataflow URL listing the bulk file.
        label: The bulk file's label in the dataflow's annotations.
        observed_url: The bulk file's URL when its source was observed.
        observed_version: The bulk file's version when its source was observed.
        download: oda_reader's bulk download function for the file.
        downloaded_name: Name of the file `download` writes.
        release_date: Date the OECD released the bulk file, from its URL and version.

    Raises:
        ValueError: If the bulk file's URL or version differs from the observed one,
            before or after the download.
    """

    def check_observed() -> None:
        url, version = resolve_bulk_file(flow_url=flow_url, label=label)
        if (url, version) != (observed_url, observed_version):
            raise ValueError(
                f"Bulk file {label!r} moved from {observed_url} at {observed_version} "
                f"to {url} at {version} after it was observed: observe it again"
            )

    # Check before downloading. The cache-bypassing resolve refreshes
    # oda_reader's cached annotation, so the download, which re-resolves the URL
    # without a bypass, sees the version observed.
    check_observed()
    released = release_date(observed_url, observed_version)
    download(save_to_path=str(file_path.parent), use_raw_cache=False)
    (file_path.parent / downloaded_name).rename(file_path)
    # The URL stays stable across a republish, so a republish during the
    # download shows only in a lookup after it.
    try:
        check_observed()
    except ValueError:
        file_path.unlink()
        raise
    return FetchedRelease(
        release_dates={"source": released}, metadata={"version": observed_version}
    )
