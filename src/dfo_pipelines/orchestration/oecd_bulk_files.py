from collections.abc import Callable
from datetime import date

import dagster as dg
from upath import UPath

from dfo_pipelines.extraction.oecd import resolve_bulk_file


def observe_bulk_file(*, flow_url: str, label: str) -> dg.ObserveResult:
    """Observe the URL and version of the bulk file `label` lists at `flow_url`"""
    url, version = resolve_bulk_file(flow_url=flow_url, label=label)
    return dg.ObserveResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(url),
            "version": dg.MetadataValue.text(version),
        },
        data_version=dg.DataVersion(f"{url};{version}"),
    )


def fetch_bulk_file(
    file_path: UPath,
    *,
    flow_url: str,
    label: str,
    download: Callable[..., object],
    downloaded_name: str,
    release_date: Callable[[str, str], date],
) -> tuple[dict[str, date], dict[str, dg.MetadataValue]]:
    """Download the bulk file `label` lists at `flow_url` to `file_path`

    Args:
        file_path: Path the raw file asset writes `data.parquet` to.
        flow_url: The OECD dataflow URL listing the bulk file.
        label: The bulk file's label in the dataflow's annotations.
        download: oda_reader's bulk download function for the file.
        downloaded_name: Name of the file `download` writes.
        release_date: Date the OECD released the bulk file, from its URL and version.
    """
    # Resolve before downloading. The cache-bypassing resolve refreshes
    # oda_reader's cached annotation, so the download, which re-resolves the URL
    # without a bypass, sees the version observed.
    url, version = resolve_bulk_file(flow_url=flow_url, label=label)
    released = release_date(url, version)
    download(save_to_path=str(file_path.parent), use_raw_cache=False)
    (file_path.parent / downloaded_name).rename(file_path)
    return {"source": released}, {"version": dg.MetadataValue.text(version)}
