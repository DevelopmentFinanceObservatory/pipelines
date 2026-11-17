from datetime import date
from email.utils import parsedate_to_datetime

import httpx
from oda_reader.download.download_tools import (
    DEFAULT_HEADERS,
    get_bulk_file_url_with_version,
)

RELEASE_DATE_TIMEOUT_SECONDS = 30


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
