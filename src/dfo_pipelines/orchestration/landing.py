from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime

import dagster as dg
import pyarrow.parquet as pq
from upath import UPath

from dfo_pipelines.extraction.metadata import FetchedRelease, write_extraction_metadata
from dfo_pipelines.orchestration.automation import EAGER_WHEN_CHECKS_PASS
from dfo_pipelines.orchestration.resources import UPathFileStoreResource


def _landing_directory(
    landing_store: UPathFileStoreResource, *, path: str, downloaded_at: datetime
) -> UPath:
    """Directory the landing file of a download at `downloaded_at` is written to"""
    return landing_store.build_date_partitioned_path(
        path, partition_date=downloaded_at.date()
    )


def landing_result(
    path: UPath,
    *,
    metadata: Mapping[str, dg.MetadataValue],
    check_results: Sequence[dg.AssetCheckResult],
) -> dg.MaterializeResult:
    """Result of writing a source's landing file or directory to `path`, with its checks"""
    return dg.MaterializeResult(
        value=str(path),
        metadata={"dagster/uri": dg.MetadataValue.text(str(path)), **metadata},
        check_results=check_results,
    )


def build_landing_asset(
    *,
    group: str,
    dataset: str,
    title: str,
    fetch: Callable[[UPath], FetchedRelease],
    retry_policy: dg.RetryPolicy | None = None,
) -> dg.AssetsDefinition:
    """The `<group>_landing` asset writing a source's download as `data.parquet`

    Args:
        group: Group of the source, naming the asset and its `<group>_source` dep.
        dataset: Directory under the group the landing file is written to.
        title: Name of the source in the description and logs, such as "OECD DAC1".
        fetch: Writes the download to the path it is given, putting any working
            files in that path's directory and nowhere else. Each of its release
            dates is recorded as `<name>_release_date`.
        retry_policy: Retry policy of the asset.
    """
    name = f"{group}_landing"

    @dg.asset(
        name=name,
        description=f"Landing {title} file and metadata",
        deps=[dg.AssetKey(f"{group}_source")],
        group_name=group,
        kinds={"file", "parquet"},
        automation_condition=EAGER_WHEN_CHECKS_PASS,
        check_specs=[
            dg.AssetCheckSpec(name="file_not_empty", asset=name, blocking=True)
        ],
        retry_policy=retry_policy,
    )
    # Left unannotated, since a MaterializeResult return annotation types the
    # output as Nothing and the IO manager then rejects the path returned as its value.
    def landing(
        context: dg.AssetExecutionContext, landing_store: UPathFileStoreResource
    ):
        downloaded_at = datetime.now(UTC)
        directory = _landing_directory(
            landing_store, path=f"{group}/{dataset}", downloaded_at=downloaded_at
        )
        directory.mkdir(parents=True, exist_ok=True)
        file_path = directory / "data.parquet"
        release_dates, source_metadata = fetch(file_path)
        with file_path.open("rb") as handle:
            rows = pq.read_metadata(handle).num_rows
        write_extraction_metadata(
            directory, downloaded_at=downloaded_at, release_dates=release_dates
        )
        file_size = file_path.stat().st_size
        context.log.info(f"Wrote {title} data to {file_path}")
        return landing_result(
            file_path,
            metadata={
                "file_size": dg.MetadataValue.int(file_size),
                "dagster/row_count": dg.MetadataValue.int(rows),
                **source_metadata,
                **{
                    f"{release}_release_date": dg.MetadataValue.text(
                        release_date.isoformat()
                    )
                    for release, release_date in release_dates.items()
                },
            },
            check_results=[
                dg.AssetCheckResult(check_name="file_not_empty", passed=(rows > 0))
            ],
        )

    return landing
