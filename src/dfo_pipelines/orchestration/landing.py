import csv
from collections.abc import Callable, Mapping, Sequence, Set
from datetime import UTC, datetime
from pathlib import PurePosixPath
from types import MappingProxyType

import dagster as dg
import pyarrow.parquet as pq
from upath import UPath

from dfo_pipelines.data_commons.mcf import count_mcf_nodes
from dfo_pipelines.extraction.metadata import FetchedRelease, write_extraction_metadata
from dfo_pipelines.orchestration.automation import EAGER_WHEN_CHECKS_PASS
from dfo_pipelines.orchestration.resources import UPathFileStoreResource

# Dagster kind of each file extension that has one
_FILE_KINDS = {".parquet": "parquet", ".csv": "csv"}


def _landing_directory(
    landing_file_store: UPathFileStoreResource, *, path: str, downloaded_at: datetime
) -> UPath:
    """Directory a download at `downloaded_at` is written to"""
    return landing_file_store.build_date_partitioned_path(
        path, partition_date=downloaded_at.date()
    )


def _count_rows(path: UPath) -> int | None:
    """Data rows in a parquet or CSV file, nodes in an MCF file, or None otherwise"""
    match path.suffix:
        case ".parquet":
            with path.open("rb") as handle:
                return pq.read_metadata(handle).num_rows
        case ".csv":
            with path.open(newline="") as handle:
                return max(sum(1 for _ in csv.reader(handle)) - 1, 0)
        case ".mcf":
            return count_mcf_nodes(path)
        case _:
            return None


def landing_result(
    path: UPath,
    *,
    metadata: Mapping[str, dg.MetadataValue],
    check_results: Sequence[dg.AssetCheckResult],
) -> dg.MaterializeResult:
    """Result of writing a source's landing directory to `path`, with its checks"""
    return dg.MaterializeResult(
        value=str(path),
        metadata={"dagster/uri": dg.MetadataValue.text(str(path)), **metadata},
        check_results=check_results,
    )


def build_landing_asset(
    *,
    group: str,
    source: dg.SourceAsset,
    dataset: str,
    title: str,
    files: Sequence[str],
    fetch: Callable[[dg.AssetExecutionContext, UPath], FetchedRelease],
    checks: Mapping[str, Callable[[UPath], bool]] = MappingProxyType({}),
    required_resource_keys: Set[str] = frozenset(),
    retry_policy: dg.RetryPolicy | None = None,
) -> dg.AssetsDefinition:
    """The `<group>_landing` asset writing a source's files to its landing directory

    Each declared file gets a blocking `<stem>_not_empty` check: a parquet file
    has rows, a CSV file has rows after its header, an MCF file declares nodes,
    and any other file has bytes.

    Args:
        group: Group of the source, naming the asset `<group>_landing`.
        source: The observable source asset the landing depends on.
        dataset: Directory under the group the files are written to.
        title: Name of the source in the description and logs, such as "OECD DAC1".
        files: Names of the files the fetch writes to the landing directory.
        fetch: Writes the files to the landing directory it is given, putting any
            working files there and nowhere else. Each of its release dates is
            recorded as `<name>_release_date`.
        checks: Further blocking checks on the landing directory, by name.
        required_resource_keys: Resources the fetch reads from its context.
        retry_policy: Retry policy of the asset.
    """
    name = f"{group}_landing"
    stems = [PurePosixPath(file).stem for file in files]
    file_kinds = {
        _FILE_KINDS[suffix]
        for file in files
        if (suffix := PurePosixPath(file).suffix) in _FILE_KINDS
    }

    @dg.asset(
        name=name,
        description=f"{title} download and its metadata",
        deps=[source],
        group_name=group,
        kinds={"file", *file_kinds},
        automation_condition=EAGER_WHEN_CHECKS_PASS,
        check_specs=[
            dg.AssetCheckSpec(name=check_name, asset=name, blocking=True)
            for check_name in [f"{stem}_not_empty" for stem in stems] + list(checks)
        ],
        required_resource_keys={"landing_file_store", *required_resource_keys},
        retry_policy=retry_policy,
    )
    # Left unannotated, since a MaterializeResult return annotation types the
    # output as Nothing and the IO manager then rejects the path returned as its value.
    def landing(context: dg.AssetExecutionContext):
        downloaded_at = datetime.now(UTC)
        directory = _landing_directory(
            context.resources.landing_file_store,
            path=f"{group}/{dataset}",
            downloaded_at=downloaded_at,
        )
        directory.mkdir(parents=True, exist_ok=True)
        release_dates, source_metadata = fetch(context, directory)
        paths = {
            stem: directory / file for stem, file in zip(stems, files, strict=True)
        }
        # Read before recording the download, so a missing or unreadable file
        # leaves no record claiming one happened.
        sizes = {stem: path.stat().st_size for stem, path in paths.items()}
        rows = {
            stem: count
            for stem, path in paths.items()
            if (count := _count_rows(path)) is not None
        }
        write_extraction_metadata(
            directory, downloaded_at=downloaded_at, release_dates=release_dates
        )
        context.log.info(f"Wrote {title} data to {directory}")

        metadata: dict[str, dg.MetadataValue] = {}
        if sizes:
            metadata["file_size"] = dg.MetadataValue.int(sum(sizes.values()))
        if rows:
            metadata["dagster/row_count"] = dg.MetadataValue.int(sum(rows.values()))
        if len(rows) > 1:
            metadata |= {
                f"{stem}_row_count": dg.MetadataValue.int(count)
                for stem, count in rows.items()
            }
        return landing_result(
            directory,
            metadata={
                **metadata,
                **source_metadata,
                **{
                    f"{release}_release_date": dg.MetadataValue.text(
                        release_date.isoformat()
                    )
                    for release, release_date in release_dates.items()
                },
            },
            check_results=[
                dg.AssetCheckResult(
                    check_name=f"{stem}_not_empty",
                    passed=rows[stem] > 0 if stem in rows else size > 0,
                )
                for stem, size in sizes.items()
            ]
            + [
                dg.AssetCheckResult(check_name=check_name, passed=check(directory))
                for check_name, check in checks.items()
            ],
        )

    return landing
