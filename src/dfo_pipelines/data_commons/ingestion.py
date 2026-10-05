"""Ingest a Data Commons import bundle into the knowledge graph.

The graph reads each import's bundle from its own folder in the ingestion
bucket, so ingesting means syncing the bundle into that folder and starting the
workflow that reads it. An ingestion rebuilds every node and edge carrying the
import's provenance and leaves the rest of the graph alone, which is why the
folder holds that import's current bundle and nothing else.
"""

import logging
from pathlib import Path

from dcp_tools.gcp_utilities.clients import get_gcs_client
from dcp_tools.gcp_utilities.ingestion import run_ingestion_workflow
from dcp_tools.gcp_utilities.settings import KGSettings
from dcp_tools.gcp_utilities.storage import sync_directory_to_gcs
from upath import UPath


def ingest_bundle(
    bundle: UPath, *, import_name: str, settings: KGSettings, log: logging.Logger
) -> str:
    """Sync a bundle into the import's folder in the ingestion bucket and ingest it.

    The sync removes anything in the folder that the bundle does not contain, so
    the graph is built from what the export wrote rather than from that plus
    whatever an earlier bundle left behind. The workflow is only started: it runs
    on for as long as the ingestion takes, after this returns.

    Args:
        bundle: Local directory the export wrote the bundle to.
        import_name: Name of the import, which names its folder in the bucket.
        settings: Knowledge graph the bundle is ingested into.
        log: Logger to use, e.g. an asset's `context.log`.

    Returns:
        str: URI of the folder the bundle was synced to.

    Raises:
        ValueError: If the bundle is not on local disk, or holds no files to upload.
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

    folder = f"{settings.gcs_input_folder_path}/{import_name}"
    bucket = get_gcs_client(settings.gcp_credentials).bucket(settings.gcs_bucket_name)
    sync_directory_to_gcs(bucket, directory, gcs_folder_name=folder)
    destination = f"gs://{settings.gcs_bucket_name}/{folder}"
    log.info(f"Synced {bundle} to {destination}")

    log.info(f"Starting the ingestion workflow for {import_name}")
    run_ingestion_workflow(settings, imports=import_name)
    return destination
