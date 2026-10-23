"""Ingest a Data Commons import bundle into the knowledge graph.

The graph reads each import's bundle from its own folder in the ingestion
bucket, so ingesting means syncing the bundle into that folder and starting the
workflow that reads it. An ingestion rebuilds every node and edge carrying the
import's provenance and leaves the rest of the graph alone, which is why the
folder holds that import's current bundle and nothing else.

The workflow runs for as long as the ingestion takes, which is longer than an
asset should wait. Its execution is watched for its first minutes only, which is
when a bad request or a failed launch shows.
"""

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from dcp_tools.gcp_utilities.clients import get_gcs_client
from dcp_tools.gcp_utilities.ingestion import (
    IngestionExecutionState,
    check_ingestion_workflow_contract,
    get_ingestion_execution_state,
    run_ingestion_workflow,
)
from dcp_tools.gcp_utilities.settings import KGSettings
from dcp_tools.gcp_utilities.storage import sync_directory_to_gcs
from upath import UPath

# Workflow execution states that end an execution without ingesting it.
FAILED_STATES = frozenset({"FAILED", "CANCELLED"})


class IngestionExecutionError(RuntimeError):
    """The ingestion workflow's execution failed or was cancelled."""


@dataclass(frozen=True, kw_only=True)
class Ingestion:
    """An ingestion that has started.

    Attributes:
        destination: URI of the folder the bundle was synced to.
        execution_name: Resource name of the workflow execution ingesting it.
    """

    destination: str
    execution_name: str


def watch_execution_start(
    settings: KGSettings,
    execution_name: str,
    *,
    log: logging.Logger,
    watch_seconds: float,
    poll_seconds: float,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> IngestionExecutionState:
    """Poll an ingestion's execution until it succeeds or the watch ends.

    Args:
        settings: Knowledge graph the execution ingests into.
        execution_name: Resource name of the execution.
        log: Logger to use, e.g. an asset's `context.log`.
        watch_seconds: How long to poll for before leaving the execution running.
        poll_seconds: How long to wait between polls.
        sleep: Waits the given number of seconds.
        clock: Monotonic time in seconds.

    Returns:
        IngestionExecutionState: The last state read, `SUCCEEDED` or still running.

    Raises:
        IngestionExecutionError: If the execution fails or is cancelled while
            watched.
    """
    deadline = clock() + watch_seconds
    while True:
        execution = get_ingestion_execution_state(settings, execution_name)
        if execution.state in FAILED_STATES:
            raise IngestionExecutionError(
                f"Ingestion execution {execution_name} ended {execution.state}: "
                f"{execution.error}"
            )
        if execution.state == "SUCCEEDED" or clock() >= deadline:
            log.info(
                f"Ingestion execution {execution_name} is {execution.state}"
                f" at steps {list(execution.current_steps)}"
            )
            return execution
        sleep(poll_seconds)


def ingest_bundle(
    bundle: UPath,
    *,
    import_name: str,
    settings: KGSettings,
    log: logging.Logger,
    watch_seconds: float = 120,
    poll_seconds: float = 5,
) -> Ingestion:
    """Sync a bundle into the import's folder in the ingestion bucket and ingest it.

    The sync removes anything in the folder that the bundle does not contain, so
    the graph is built from what the export wrote rather than from that plus
    whatever an earlier bundle left behind. The workflow is checked before the
    sync, so a workflow dcp-tools cannot start leaves the folder untouched. Its
    execution is watched for `watch_seconds` and runs on after this returns.

    Args:
        bundle: Local directory the export wrote the bundle to.
        import_name: Name of the import, which names its folder in the bucket.
        settings: Knowledge graph the bundle is ingested into.
        log: Logger to use, e.g. an asset's `context.log`.
        watch_seconds: How long to watch the execution for after starting it.
        poll_seconds: How long to wait between reads of its state.

    Returns:
        Ingestion: Where the bundle was synced to and the execution ingesting it.

    Raises:
        ValueError: If the bundle is not on local disk, or holds no files to upload.
        WorkflowContractError: If the workflow is not one dcp-tools can start,
            raised before anything is synced.
        IngestionExecutionError: If the execution fails or is cancelled while
            watched.
    """
    # dcp-tools uploads from local disk.
    if bundle.protocol not in ("", "file"):
        raise ValueError(f"The bundle must be on local disk, not at {bundle}")
    directory = Path(bundle.path)
    # Syncing nothing would leave the folder as it was, and the ingestion would
    # then publish the previous bundle as if it were this one. dcp-tools uploads
    # only these file types and skips everything else in the bundle.
    if not any(
        path.is_file() and path.suffix in {".csv", ".json", ".mcf"}
        for path in directory.rglob("*")
    ):
        raise ValueError(f"Found no bundle to ingest at {bundle}")

    check_ingestion_workflow_contract(settings)

    folder = f"{settings.gcs_input_folder_path}/{import_name}"
    client = get_gcs_client(settings.gcp_credentials, project=settings.gcp_project_id)
    bucket = client.bucket(settings.gcs_bucket_name)
    sync_directory_to_gcs(bucket, directory, gcs_folder_name=folder)
    destination = f"gs://{settings.gcs_bucket_name}/{folder}"
    log.info(f"Synced {bundle} to {destination}")

    log.info(f"Starting the ingestion workflow for {import_name}")
    execution_name = run_ingestion_workflow(
        settings, imports=import_name, skip_workflow_contract_check=True
    )
    log.info(f"Started ingestion execution {execution_name}")
    watch_execution_start(
        settings,
        execution_name,
        log=log,
        watch_seconds=watch_seconds,
        poll_seconds=poll_seconds,
    )
    return Ingestion(destination=destination, execution_name=execution_name)
