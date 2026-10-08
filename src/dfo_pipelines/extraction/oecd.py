from datetime import date
from email.utils import parsedate_to_datetime

import httpx
from oda_reader.download.download_tools import DEFAULT_HEADERS

RELEASE_DATE_TIMEOUT_SECONDS = 30


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
