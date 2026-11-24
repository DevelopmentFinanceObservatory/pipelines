import csv
import logging
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Any

import dagster as dg
import pyarrow.parquet as pq
from upath import UPath

from dfo_pipelines.data_commons.mcf import count_mcf_nodes
from dfo_pipelines.extraction.metadata import FetchedRelease, write_extraction_metadata
from dfo_pipelines.orchestration.automation import EAGER_WHEN_CHECKS_PASS
from dfo_pipelines.orchestration.resources import UPathFileStoreResource

# Dagster kind of each file extension that has one
_FILE_KINDS = {".parquet": "parquet", ".csv": "csv"}


@dataclass(frozen=True)
class Observation:
    """A source asset's observation: its data version and the plain value of each
    metadata entry"""

    data_version: str
    metadata: Mapping[str, Any]


def _latest_observation(
    instance: dg.DagsterInstance, source: dg.SourceAsset
) -> Observation:
    """The source's latest observation

    Raises:
        ValueError: If the source has no observation with a data version.
    """
    record = instance.get_latest_data_version_record(source.key, is_source=True)
    observation = record.asset_observation if record else None
    if observation is None or observation.data_version is None:
        raise ValueError(
            f"{source.key.to_user_string()} has no observation; observe it first"
        )
    return Observation(
        data_version=observation.data_version,
        metadata={key: value.value for key, value in observation.metadata.items()},
    )


def _create_landing_directory(
    landing_file_store: UPathFileStoreResource, *, path: str, downloaded_at: datetime
) -> UPath:
    """Create the directory a download at `downloaded_at` is written to

    Raises:
        FileExistsError: If the directory exists, since each is written once.
    """
    directory = (
        UPath(landing_file_store.base_path)
        / path
        / downloaded_at.strftime("%Y%m%dT%H%M%SZ")
    )
    if directory.exists():
        raise FileExistsError(
            f"{directory} exists, and a landing directory is written once"
        )
    directory.mkdir(parents=True)
    return directory


@contextmanager
def _remove_on_exception(directory: UPath, log: logging.Logger) -> Iterator[None]:
    """Remove `directory` if the block raises, then re-raise"""
    try:
        yield
    except BaseException:
        # Raise the failure that stopped the write, not one from cleaning up.
        try:
            directory.rmdir(recursive=True)
        except Exception as error:
            log.warning(f"Could not remove {directory}: {error}")
        raise


def _file_sizes(paths: Mapping[str, UPath]) -> dict[str, int]:
    """Size in bytes of each file, by stem"""
    return {stem: path.stat().st_size for stem, path in paths.items()}


def _row_counts(paths: Mapping[str, UPath]) -> dict[str, int]:
    """Row count of each file that has one, by stem"""
    return {
        stem: count
        for stem, path in paths.items()
        if (count := _count_rows(path)) is not None
    }


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


def _landing_result(
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


def _files_not_empty(
    files: Sequence[str],
    stems: Sequence[str],
    *,
    sizes: Mapping[str, int],
    rows: Mapping[str, int],
) -> list[dg.AssetCheckResult]:
    """The `files_not_empty` result, naming each declared file that is empty"""
    if not files:
        return []
    empty_files = [
        file
        for file, stem in zip(files, stems, strict=True)
        if (rows[stem] if stem in rows else sizes[stem]) == 0
    ]
    return [
        dg.AssetCheckResult(
            check_name="files_not_empty",
            passed=not empty_files,
            metadata={"empty_files": dg.MetadataValue.json(empty_files)},
        )
    ]


def _named_check_results(
    checks: Mapping[str, Callable[[UPath], bool]], directory: UPath
) -> list[dg.AssetCheckResult]:
    """Result of each named check on the landing directory"""
    return [
        dg.AssetCheckResult(check_name=check_name, passed=check(directory))
        for check_name, check in checks.items()
    ]


def _size_and_row_count_metadata(
    sizes: Mapping[str, int], rows: Mapping[str, int]
) -> dict[str, dg.MetadataValue]:
    """Total size and row count of the files, and each file's row count when several
    have one"""
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
    return metadata


def _release_date_metadata(
    release_dates: Mapping[str, date],
) -> dict[str, dg.MetadataValue]:
    """Each release date as `<name>_release_date`"""
    return {
        f"{release}_release_date": dg.MetadataValue.text(release_date.isoformat())
        for release, release_date in release_dates.items()
    }


def build_landing_asset(
    *,
    group: str,
    source: dg.SourceAsset,
    dataset: str,
    title: str,
    files: Sequence[str],
    fetch: Callable[[UPath, Observation], FetchedRelease],
    checks: Mapping[str, Callable[[UPath], bool]] = MappingProxyType({}),
    retry_policy: dg.RetryPolicy | None = None,
) -> dg.AssetsDefinition:
    """The `<group>_landing` asset writing a source's files to its landing directory

    A blocking `files_not_empty` check passes when every declared file is not
    empty: a parquet file has rows, a CSV file has rows after its header, an MCF
    file declares nodes, and any other file has bytes. Its metadata names the
    empty ones.

    The asset passes its fetch the source's latest observation, and fails with
    a ValueError when the source has none.

    Each download is written once, to a directory named for its UTC instant, with
    the extraction metadata written last.

    Args:
        group: Group of the source, naming the asset `<group>_landing`.
        source: The observable source asset the landing depends on.
        dataset: Directory under the group the files are written to.
        title: Name of the source in the description and logs, such as "OECD DAC1".
        files: Names of the files the fetch writes to the landing directory.
        fetch: Writes the files to the landing directory it is given, putting any
            working files there and nowhere else, and landing the version the
            observation it is given saw. Each of its release dates is recorded
            as `<name>_release_date`.
        checks: Further blocking checks on the landing directory, by name.
        retry_policy: Retry policy of the asset.

    Raises:
        ValueError: If two declared files share a stem, which names their row count.
    """
    name = f"{group}_landing"
    stems = [PurePosixPath(file).stem for file in files]
    if shared := sorted(stem for stem, count in Counter(stems).items() if count > 1):
        raise ValueError(f"Declared files share the stems {shared}; give each its own")
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
            for check_name in (["files_not_empty"] if files else []) + list(checks)
        ],
        required_resource_keys={"landing_file_store"},
        retry_policy=retry_policy,
    )
    # Left unannotated, since a MaterializeResult return annotation types the
    # output as Nothing and the IO manager then rejects the path returned as its value.
    def landing(context: dg.AssetExecutionContext):
        observation = _latest_observation(context.instance, source)
        downloaded_at = datetime.now(UTC)
        directory = _create_landing_directory(
            context.resources.landing_file_store,
            path=f"{group}/{dataset}",
            downloaded_at=downloaded_at,
        )
        with _remove_on_exception(directory, context.log):
            release_dates, source_metadata = fetch(directory, observation)
            paths = {
                stem: directory / file for stem, file in zip(stems, files, strict=True)
            }
            sizes = _file_sizes(paths)
            rows = _row_counts(paths)
            check_results = _files_not_empty(
                files, stems, sizes=sizes, rows=rows
            ) + _named_check_results(checks, directory)
            write_extraction_metadata(
                directory,
                downloaded_at=downloaded_at,
                run_id=context.run.run_id,
                release_dates=release_dates,
            )
        context.log.info(f"Wrote {title} data to {directory}")
        return _landing_result(
            directory,
            metadata={
                **_size_and_row_count_metadata(sizes, rows),
                **source_metadata,
                **_release_date_metadata(release_dates),
            },
            check_results=check_results,
        )

    return landing
