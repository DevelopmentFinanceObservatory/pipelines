import logging
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dcp_tools.gcp_utilities.ingestion import WorkflowContractError
from dcp_tools.gcp_utilities.settings import KGSettings
from upath import UPath

from dfo_pipelines.data_commons.ingestion import ingest_bundle
from dfo_pipelines.defs.resources import KnowledgeGraphResource

LOG = logging.getLogger(__name__)

IMPORT_NAME = "test_import"


@pytest.fixture
def gcp() -> Iterator[MagicMock]:
    """dcp-tools' GCP calls, recorded in the order they are made"""
    calls = MagicMock()
    with (
        patch(
            "dfo_pipelines.data_commons.ingestion.check_ingestion_workflow_contract",
            calls.check_ingestion_workflow_contract,
        ),
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


def ingest(bundle: Path, settings: KGSettings) -> str:
    """Ingest a bundle into the test knowledge graph"""
    return ingest_bundle(
        UPath(bundle), import_name=IMPORT_NAME, settings=settings, log=LOG
    )


def test_ingest_syncs_the_bundle_into_the_import_folder_then_ingests_it(
    tmp_path: Path, gcp: MagicMock, knowledge_graph: KnowledgeGraphResource
) -> None:
    """Test the sync lands in the import's own folder before the workflow starts"""
    bundle = write_bundle(tmp_path / "bundle")
    settings = knowledge_graph.settings
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
    assert trigger == (
        "run_ingestion_workflow",
        (settings,),
        {"imports": IMPORT_NAME, "skip_workflow_contract_check": True},
    )
    assert destination == f"gs://{settings.gcs_bucket_name}/{folder}"


def test_ingest_checks_the_workflow_contract_before_the_sync(
    tmp_path: Path, gcp: MagicMock, knowledge_graph: KnowledgeGraphResource
) -> None:
    """Test the workflow is confirmed as one dcp-tools can start before any upload"""
    bundle = write_bundle(tmp_path / "bundle")
    settings = knowledge_graph.settings

    ingest(bundle, settings)

    check, sync = [
        call
        for call in gcp.mock_calls
        if call[0] in ("check_ingestion_workflow_contract", "sync_directory_to_gcs")
    ]
    assert check == ("check_ingestion_workflow_contract", (settings,), {})
    assert sync[0] == "sync_directory_to_gcs"


def test_ingest_uploads_nothing_when_the_workflow_contract_fails(
    tmp_path: Path, gcp: MagicMock, knowledge_graph: KnowledgeGraphResource
) -> None:
    """Test a workflow from an older stack stops the ingestion before any upload"""
    bundle = write_bundle(tmp_path / "bundle")
    gcp.check_ingestion_workflow_contract.side_effect = WorkflowContractError(
        "older stack"
    )

    with pytest.raises(WorkflowContractError, match="older stack"):
        ingest(bundle, knowledge_graph.settings)

    gcp.sync_directory_to_gcs.assert_not_called()
    gcp.run_ingestion_workflow.assert_not_called()


def test_ingest_starts_no_workflow_when_the_sync_fails(
    tmp_path: Path, gcp: MagicMock, knowledge_graph: KnowledgeGraphResource
) -> None:
    """Test a failed sync is not followed by an ingestion of the old bundle"""
    bundle = write_bundle(tmp_path / "bundle")
    gcp.sync_directory_to_gcs.side_effect = RuntimeError("upload failed")

    with pytest.raises(RuntimeError, match="upload failed"):
        ingest(bundle, knowledge_graph.settings)

    gcp.run_ingestion_workflow.assert_not_called()


@pytest.mark.parametrize(
    "contents",
    [None, [], ["subdirectory/"], ["report.txt"]],
    ids=["missing", "empty", "only-a-subdirectory", "only-files-not-uploaded"],
)
def test_ingest_refuses_a_bundle_with_no_files(
    tmp_path: Path,
    gcp: MagicMock,
    knowledge_graph: KnowledgeGraphResource,
    contents: list[str] | None,
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
        ingest(bundle, knowledge_graph.settings)

    gcp.sync_directory_to_gcs.assert_not_called()
    gcp.run_ingestion_workflow.assert_not_called()


def test_ingest_refuses_a_bundle_not_on_local_disk(
    gcp: MagicMock, knowledge_graph: KnowledgeGraphResource
) -> None:
    """Test a remote bundle fails rather than reaching dcp-tools' local upload"""
    with pytest.raises(ValueError, match="local disk"):
        ingest_bundle(
            UPath("memory://bundle"),
            import_name=IMPORT_NAME,
            settings=knowledge_graph.settings,
            log=LOG,
        )

    gcp.sync_directory_to_gcs.assert_not_called()


def test_knowledge_graph_settings_build_beside_the_project_env_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    knowledge_graph: KnowledgeGraphResource,
) -> None:
    """Test the settings build with a .env holding variables KGSettings doesn't declare"""
    (tmp_path / ".env").write_text("DUCKDB_PATH=elsewhere.duckdb\n")
    monkeypatch.chdir(tmp_path)

    settings = knowledge_graph.settings

    assert settings.gcs_bucket_name == knowledge_graph.bucket_name
    assert settings.ingestion_workflow_name == knowledge_graph.workflow_name
