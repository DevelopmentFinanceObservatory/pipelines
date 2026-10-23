import logging
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dcp_tools.gcp_utilities.settings import KGSettings
from upath import UPath

from dfo_pipelines.data_commons.ingestion import ingest_bundle
from dfo_pipelines.defs.resources import KnowledgeGraphResource

LOG = logging.getLogger(__name__)

IMPORT_NAME = "test_import"

KNOWLEDGE_GRAPH = KnowledgeGraphResource(
    project_id="test-project",
    region="test-region",
    bucket_name="test-bucket",
    workflow_name="test-workflow",
)


@pytest.fixture
def gcp() -> Iterator[MagicMock]:
    """dcp-tools' GCP calls, recorded in the order they are made"""
    calls = MagicMock()
    with (
        patch(
            "dfo_pipelines.data_commons.ingestion.get_gcs_client",
            calls.get_gcs_client,
        ),
        patch(
            "dfo_pipelines.data_commons.ingestion.sync_directory_to_gcs",
            calls.sync_directory_to_gcs,
        ),
        patch(
            "dfo_pipelines.data_commons.ingestion.run_ingestion_workflow",
            calls.run_ingestion_workflow,
        ),
    ):
        yield calls


def write_bundle(directory: Path) -> Path:
    """Write a minimal bundle, as an export would"""
    directory.mkdir(parents=True)
    (directory / "config.json").write_text("{}")
    (directory / "nodes.mcf").write_text("Node: dcid:Test\n")
    return directory


def ingest(bundle: Path, settings: KGSettings | None = None) -> str:
    """Ingest a bundle into the test knowledge graph"""
    return ingest_bundle(
        UPath(bundle),
        import_name=IMPORT_NAME,
        settings=settings or KNOWLEDGE_GRAPH.settings,
        log=LOG,
    )


def test_ingest_syncs_the_bundle_into_the_import_folder_then_ingests_it(
    tmp_path: Path, gcp: MagicMock
) -> None:
    """Test the sync lands in the import's own folder before the workflow starts"""
    bundle = write_bundle(tmp_path / "bundle")
    settings = KNOWLEDGE_GRAPH.settings
    folder = f"{settings.gcs_input_folder_path}/{IMPORT_NAME}"

    destination = ingest(bundle, settings)

    bucket = gcp.get_gcs_client.return_value.bucket
    bucket.assert_called_once_with(settings.gcs_bucket_name)
    sync, trigger = [
        call
        for call in gcp.mock_calls
        if call[0] in ("sync_directory_to_gcs", "run_ingestion_workflow")
    ]
    assert sync == (
        "sync_directory_to_gcs",
        (bucket.return_value, bundle),
        {"gcs_folder_name": folder},
    )
    assert trigger == ("run_ingestion_workflow", (settings,), {"imports": IMPORT_NAME})
    assert destination == f"gs://{settings.gcs_bucket_name}/{folder}"


def test_ingest_starts_no_workflow_when_the_sync_fails(
    tmp_path: Path, gcp: MagicMock
) -> None:
    """Test a failed sync is not followed by an ingestion of the old bundle"""
    bundle = write_bundle(tmp_path / "bundle")
    gcp.sync_directory_to_gcs.side_effect = RuntimeError("upload failed")

    with pytest.raises(RuntimeError, match="upload failed"):
        ingest(bundle)

    gcp.run_ingestion_workflow.assert_not_called()


@pytest.mark.parametrize(
    "contents",
    [None, [], ["subdirectory/"], ["report.txt"]],
    ids=["missing", "empty", "only-a-subdirectory", "only-files-not-uploaded"],
)
def test_ingest_refuses_a_bundle_with_no_files(
    tmp_path: Path, gcp: MagicMock, contents: list[str] | None
) -> None:
    """Test nothing reaches GCP when there is no bundle to ingest"""
    bundle = tmp_path / "bundle"
    if contents is not None:
        bundle.mkdir()
        for name in contents:
            if name.endswith("/"):
                (bundle / name).mkdir()
            else:
                (bundle / name).write_text("")

    with pytest.raises(ValueError, match="Found no bundle"):
        ingest(bundle)

    gcp.sync_directory_to_gcs.assert_not_called()
    gcp.run_ingestion_workflow.assert_not_called()


def test_ingest_refuses_a_bundle_not_on_local_disk(gcp: MagicMock) -> None:
    """Test a remote bundle fails rather than reaching dcp-tools' local upload"""
    with pytest.raises(ValueError, match="local disk"):
        ingest_bundle(
            UPath("memory://bundle"),
            import_name=IMPORT_NAME,
            settings=KNOWLEDGE_GRAPH.settings,
            log=LOG,
        )

    gcp.sync_directory_to_gcs.assert_not_called()


def test_knowledge_graph_settings_ignore_the_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test the settings build from the resource alone, with a .env file present

    KGSettings would otherwise read .env and reject every variable it does not
    declare.
    """
    (tmp_path / ".env").write_text("DUCKDB_PATH=elsewhere.duckdb\n")
    monkeypatch.chdir(tmp_path)

    settings = KNOWLEDGE_GRAPH.settings

    assert settings.gcs_bucket_name == KNOWLEDGE_GRAPH.bucket_name
    assert settings.ingestion_workflow_name == KNOWLEDGE_GRAPH.workflow_name
