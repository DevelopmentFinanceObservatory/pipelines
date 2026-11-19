from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import NamedTuple

import dagster as dg
import pyarrow.parquet as pq
from upath import UPath

from dfo_pipelines.extraction.metadata import (
    ExtractionMetadata,
    FetchedRelease,
    commit_extraction_metadata,
    file_hashes,
    holds_same_download,
    read_extraction_metadata,
)
from dfo_pipelines.orchestration.automation import EAGER_WHEN_CHECKS_PASS
from dfo_pipelines.orchestration.resources import UPathFileStoreResource


class CommittedLandingDirectory(NamedTuple):
    """A landing directory holding a complete download, and what was recorded about it

    Attributes:
        directory: The directory, this run's or an earlier run's holding the same
            download.
        extraction: The extraction metadata recorded in `directory`.
        fetched: What this run's write returned.
    """

    directory: UPath
    extraction: ExtractionMetadata
    fetched: FetchedRelease


def commit_landing_directory(
    context: dg.AssetExecutionContext,
    *,
    landing_file_store: UPathFileStoreResource,
    path: str,
    write: Callable[[UPath], FetchedRelease],
) -> CommittedLandingDirectory:
    """Write a download to a directory of its own and mark it complete

    The directory is `<path>/<download time in UTC>/` in the file store. Its
    extraction metadata, holding the sha256 of each file, is written last, so a
    directory without it is incomplete. A download whose file hashes and
    release dates match those of the directory the asset's latest materialization
    returned, if a sibling of this one, is deleted and that directory returned.

    Args:
        context: Context of the landing asset.
        landing_file_store: File store the directory is written to.
        path: Path in the file store of the source's landing directories, such as
            `<group>/<dataset>`.
        write: Writes the download into the directory it is given and returns
            what it fetched.

    Raises:
        FileExistsError: If the directory exists.
    """
    downloaded_at = datetime.now(UTC)
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
    try:
        fetched = write(directory)
        files = file_hashes(directory)
        materialized_directory = _latest_sibling(context, directory)
        duplicate = materialized_directory is not None and holds_same_download(
            materialized_directory, files=files, release_dates=fetched.release_dates
        )
        if not duplicate:
            commit_extraction_metadata(
                directory,
                downloaded_at=downloaded_at,
                run_id=context.run.run_id,
                fetched=fetched,
                files=files,
            )
    except BaseException:
        # Raise the failure that stopped the write, not one from cleaning up after it.
        try:
            directory.rmdir(recursive=True)
        except Exception as error:
            context.log.warning(f"Could not remove {directory}: {error}")
        raise
    if duplicate:
        try:
            directory.rmdir(recursive=True)
        except Exception as error:
            context.log.warning(
                f"Download matches {materialized_directory}, but {directory} could "
                f"not be removed: {error}"
            )
        else:
            context.log.info(
                f"Download matches {materialized_directory}, so {directory} was deleted"
            )
        directory = materialized_directory
    extraction = read_extraction_metadata(
        directory,
        release_names=fetched.release_dates.keys(),
        landing_asset=context.asset_key.to_user_string(),
    )
    return CommittedLandingDirectory(directory, extraction, fetched)


def _latest_sibling(
    context: dg.AssetExecutionContext, directory: UPath
) -> UPath | None:
    """Directory the asset's latest materialization returned, if a sibling of
    `directory`"""
    event = context.instance.get_latest_materialization_event(context.asset_key)
    if event is None or event.asset_materialization is None:
        return None
    uri = event.asset_materialization.metadata.get("dagster/uri")
    if not isinstance(uri, dg.TextMetadataValue):
        return None
    materialized_directory = UPath(uri.value)
    if materialized_directory.parent != directory.parent:
        return None
    return materialized_directory


def _parquet_rows(file_path: UPath) -> int:
    with file_path.open("rb") as handle:
        return pq.read_metadata(handle).num_rows


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
    dataset: str,
    title: str,
    fetch: Callable[[UPath], FetchedRelease],
    retry_policy: dg.RetryPolicy | None = None,
) -> dg.AssetsDefinition:
    """The `<group>_landing` asset writing a download as `<dataset>.parquet`

    Args:
        group: Group of the source, naming the asset and its `<group>_source` dep.
        dataset: Directory under the group the download is written to, and the
            download's stem.
        title: Name of the source in the description and logs, such as "OECD DAC1".
        fetch: Writes the download to the path it is given, putting any working
            files in that path's directory and nowhere else. Each of its release
            dates, of which it returns at least one, is recorded as
            `<name>_release_date`, and its metadata's `version`, if any, as `version`.
        retry_policy: Retry policy of the asset.
    """
    name = f"{group}_landing"

    @dg.asset(
        name=name,
        description=f"{title} download and its metadata",
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
        context: dg.AssetExecutionContext, landing_file_store: UPathFileStoreResource
    ):
        def write(directory: UPath) -> FetchedRelease:
            file_path = directory / f"{dataset}.parquet"
            fetched = fetch(file_path)
            if not fetched.release_dates:
                raise ValueError(f"The {title} fetch returned no release dates")
            # Read before the directory is committed, so an unreadable file is not.
            _parquet_rows(file_path)
            context.log.info(f"Wrote {title} data to {file_path}")
            if "version" not in fetched.metadata:
                return fetched
            return fetched._replace(
                recorded={**fetched.recorded, "version": fetched.metadata["version"]}
            )

        directory, extraction, fetched = commit_landing_directory(
            context,
            landing_file_store=landing_file_store,
            path=f"{group}/{dataset}",
            write=write,
        )
        file_path = directory / f"{dataset}.parquet"
        rows = _parquet_rows(file_path)
        file_size = file_path.stat().st_size
        return landing_result(
            directory,
            metadata={
                "file_size": dg.MetadataValue.int(file_size),
                "dagster/row_count": dg.MetadataValue.int(rows),
                **fetched.metadata,
                # The returned directory's version, an earlier run's when deduplicated
                **{
                    name: dg.MetadataValue.text(value)
                    for name, value in extraction.recorded.items()
                },
                **{
                    f"{release}_release_date": dg.MetadataValue.text(
                        release_date.isoformat()
                    )
                    for release, release_date in extraction.release_dates.items()
                },
            },
            check_results=[
                dg.AssetCheckResult(check_name="file_not_empty", passed=(rows > 0))
            ],
        )

    return landing
