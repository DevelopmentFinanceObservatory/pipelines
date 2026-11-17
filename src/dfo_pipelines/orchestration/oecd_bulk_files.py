from collections.abc import Callable
from datetime import date
from functools import partial

import dagster as dg
from upath import UPath

from dfo_pipelines.extraction.oecd import resolve_bulk_file
from dfo_pipelines.orchestration.raw_files import RawFileFetch, build_raw_file_asset


def _fetch_bulk_file(
    file_path: UPath,
    *,
    flow_url: str,
    label: str,
    download: Callable[..., object],
    downloaded_name: str,
    release_date: Callable[[str, str], date],
) -> RawFileFetch:
    """Download the bulk file `label` lists at `flow_url` to `file_path`"""
    # Resolve before downloading. The cache-bypassing resolve refreshes
    # oda_reader's cached annotation, so the download, which re-resolves the URL
    # without a bypass, sees the version observed.
    url, version = resolve_bulk_file(flow_url=flow_url, label=label)
    released = release_date(url, version)
    download(save_to_path=str(file_path.parent), use_raw_cache=False)
    (file_path.parent / downloaded_name).rename(file_path)
    return RawFileFetch(
        release_dates={"source": released},
        metadata={"version": dg.MetadataValue.text(version)},
    )


def build_oecd_bulk_file_assets(
    *,
    group: str,
    dataset: str,
    title: str,
    flow_url: str,
    label: str,
    download: Callable[..., object],
    downloaded_name: str,
    release_date: Callable[[str, str], date],
) -> tuple[dg.SourceAsset, dg.AssetsDefinition]:
    """The `<group>_source` and `<group>_raw_file` assets of an OECD bulk file

    The source observes the bulk file's URL and version daily, and the raw file
    downloads the same bulk file, so the two cannot drift apart.

    Args:
        group: Group of the source, naming both assets.
        dataset: Directory under the group the raw file is written to.
        title: Name of the source in the description and logs, such as "OECD DAC1".
        flow_url: The OECD dataflow URL listing the bulk file.
        label: The bulk file's label in the dataflow's annotations.
        download: oda_reader's bulk download function for the file.
        downloaded_name: Name of the file `download` writes.
        release_date: Date the OECD released the bulk file, from its URL and version.
    """

    @dg.observable_source_asset(
        name=f"{group}_source",
        group_name=group,
        automation_condition=dg.AutomationCondition.on_cron("@daily"),
    )
    def source() -> dg.ObserveResult:
        """Observed data version, from a daily call to the OECD API"""
        url, version = resolve_bulk_file(flow_url=flow_url, label=label)
        return dg.ObserveResult(
            metadata={
                "dagster/uri": dg.MetadataValue.text(url),
                "version": dg.MetadataValue.text(version),
            },
            data_version=dg.DataVersion(f"{url};{version}"),
        )

    fetch = partial(
        _fetch_bulk_file,
        flow_url=flow_url,
        label=label,
        download=download,
        downloaded_name=downloaded_name,
        release_date=release_date,
    )
    raw_file = build_raw_file_asset(
        group=group, dataset=dataset, title=title, fetch=fetch
    )
    return source, raw_file
