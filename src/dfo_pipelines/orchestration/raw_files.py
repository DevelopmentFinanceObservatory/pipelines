from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

import dagster as dg
import pyarrow.parquet as pq
from upath import UPath

from dfo_pipelines.extraction.metadata import write_extraction_metadata
from dfo_pipelines.orchestration.automation import EAGER_WHEN_CHECKS_PASS
from dfo_pipelines.orchestration.resources import UPathFileStoreResource


@dataclass(frozen=True)
class RawFileFetch:
    """What a source's fetch hook returns after writing `data.parquet`

    Attributes:
        release_dates: Date each named source released the fetched data, keyed by
            source name, recorded as `<name>_release_date`.
        metadata: The source's own materialization metadata.
    """

    release_dates: Mapping[str, date]
    metadata: Mapping[str, dg.MetadataValue] = field(default_factory=dict)


def _raw_file_directory(
    raw_file_store: UPathFileStoreResource, *, path: str, downloaded_at: datetime
) -> UPath:
    """Directory the raw file of a download at `downloaded_at` is written to"""
    return raw_file_store.build_date_partitioned_path(
        path, partition_date=downloaded_at.date()
    )


def build_raw_file_asset(
    *,
    group: str,
    dataset: str,
    title: str,
    fetch: Callable[[UPath], RawFileFetch],
    description: str | None = None,
    retry_policy: dg.RetryPolicy | None = None,
) -> dg.AssetsDefinition:
    """The `<group>_raw_file` asset writing a source's download as `data.parquet`

    Args:
        group: Group of the source, naming the asset and its `<group>_source` dep.
        dataset: Directory under the group the raw file is written to.
        title: Name of the source in the description and logs, such as "OECD DAC1".
        fetch: Writes the download to the path it is given, putting any working
            files in that path's directory and nowhere else.
        description: The asset's description, if not the standard one.
        retry_policy: Retry policy of the asset.
    """
    name = f"{group}_raw_file"

    @dg.asset(
        name=name,
        description=description or f"Raw {title} bulk download file and metadata",
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
    def raw_file(
        context: dg.AssetExecutionContext, raw_file_store: UPathFileStoreResource
    ):
        downloaded_at = datetime.now(UTC)
        directory = _raw_file_directory(
            raw_file_store, path=f"{group}/{dataset}", downloaded_at=downloaded_at
        )
        directory.mkdir(parents=True, exist_ok=True)
        file_path = directory / "data.parquet"
        fetched = fetch(file_path)
        with file_path.open("rb") as handle:
            rows = pq.read_metadata(handle).num_rows
        write_extraction_metadata(
            directory, downloaded_at=downloaded_at, release_dates=fetched.release_dates
        )
        file_size = file_path.stat().st_size
        context.log.info(f"Wrote {title} data to {file_path}")
        return dg.MaterializeResult(
            value=str(file_path),
            metadata={
                "dagster/uri": dg.MetadataValue.text(str(file_path)),
                "file_size": dg.MetadataValue.int(file_size),
                "dagster/row_count": dg.MetadataValue.int(rows),
                **fetched.metadata,
                **{
                    f"{release}_release_date": dg.MetadataValue.text(
                        release_date.isoformat()
                    )
                    for release, release_date in fetched.release_dates.items()
                },
            },
            check_results=[
                dg.AssetCheckResult(check_name="file_not_empty", passed=(rows > 0))
            ],
        )

    return raw_file
