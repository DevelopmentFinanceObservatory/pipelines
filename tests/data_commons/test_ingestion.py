import logging
import warnings
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from dcp_tools.gcp_utilities.ingestion import (
    IngestionExecutionState,
    WorkflowContractError,
)
from upath import UPath

from dfo_pipelines.data_commons.ingestion import (
    Ingestion,
    IngestionExecutionError,
    ingest_bundle,
    watch_execution_start,
)
from dfo_pipelines.defs.resources import KnowledgeGraphResource
from tests.helpers import EXECUTION_NAME

LOG = logging.getLogger(__name__)

IMPORT_NAME = "test_import"

SUCCEEDED = IngestionExecutionState(state="SUCCEEDED", current_steps=(), error=None)
ACTIVE = IngestionExecutionState(
    state="ACTIVE", current_steps=("main.run_preprocessing",), error=None
)


class FakeTime:
    """A clock that moves only when slept on"""

    def __init__(self) -> None:
        self.now = 0.0

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def gcp() -> Iterator[MagicMock]:
    """dcp-tools' GCP calls, recorded in the order they are made"""
    calls = MagicMock()
    calls.run_ingestion_workflow.return_value = EXECUTION_NAME
    calls.get_ingestion_execution_state.return_value = SUCCEEDED
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
        patch(
            "dfo_pipelines.data_commons.ingestion.get_ingestion_execution_state",
            calls.get_ingestion_execution_state,
        ),
    ):
        yield calls


def write_bundle(directory: Path) -> Path:
    """Write a minimal bundle, as an export would"""
    directory.mkdir(parents=True)
    (directory / "config.json").write_text("{}")
    (directory / "nodes.mcf").write_text("Node: dcid:Test\n")
    return directory


def ingest(bundle: Path, knowledge_graph: KnowledgeGraphResource) -> Ingestion:
    """Ingest a bundle into the test knowledge graph"""
    return ingest_bundle(
        UPath(bundle),
        import_name=IMPORT_NAME,
        settings=knowledge_graph.settings,
        log=LOG,
    )


def test_ingest_syncs_the_bundle_into_the_import_folder_then_ingests_it(
    tmp_path: Path, gcp: MagicMock, knowledge_graph: KnowledgeGraphResource
) -> None:
    """Test the sync lands in the import's own folder before the workflow starts"""
    bundle = write_bundle(tmp_path / "bundle")
    settings = knowledge_graph.settings
    folder = f"{settings.gcs_input_folder_path}/{IMPORT_NAME}"

    ingestion = ingest(bundle, knowledge_graph)

    gcp.get_gcs_client.assert_called_once_with(
        settings.gcp_credentials, project=settings.gcp_project_id
    )
    bucket = gcp.get_gcs_client.return_value.bucket
    bucket.assert_called_once_with(settings.gcs_bucket_name)
    steps = [
        step
        for step in gcp.mock_calls
        if step[0]
        in (
            "check_ingestion_workflow_contract",
            "sync_directory_to_gcs",
            "run_ingestion_workflow",
            "get_ingestion_execution_state",
        )
    ]
    assert steps == [
        call.check_ingestion_workflow_contract(settings),
        call.sync_directory_to_gcs(bucket.return_value, bundle, gcs_folder_name=folder),
        call.run_ingestion_workflow(
            settings, imports=IMPORT_NAME, skip_workflow_contract_check=True
        ),
        call.get_ingestion_execution_state(settings, EXECUTION_NAME),
    ]
    assert ingestion == Ingestion(
        destination=f"gs://{settings.gcs_bucket_name}/{folder}",
        execution_name=EXECUTION_NAME,
    )


def test_ingest_starts_no_workflow_when_the_sync_fails(
    tmp_path: Path, gcp: MagicMock, knowledge_graph: KnowledgeGraphResource
) -> None:
    """Test a failed sync is not followed by an ingestion of the old bundle"""
    bundle = write_bundle(tmp_path / "bundle")
    gcp.sync_directory_to_gcs.side_effect = RuntimeError("upload failed")

    with pytest.raises(RuntimeError, match="upload failed"):
        ingest(bundle, knowledge_graph)

    gcp.run_ingestion_workflow.assert_not_called()


def test_ingest_uploads_nothing_to_a_workflow_dcp_tools_refuses(
    tmp_path: Path, gcp: MagicMock, knowledge_graph: KnowledgeGraphResource
) -> None:
    """Test a workflow from the wrong stack fails the ingestion before the sync"""
    bundle = write_bundle(tmp_path / "bundle")
    gcp.check_ingestion_workflow_contract.side_effect = WorkflowContractError(
        "not DCP 1.1.6"
    )

    with pytest.raises(WorkflowContractError):
        ingest(bundle, knowledge_graph)

    gcp.sync_directory_to_gcs.assert_not_called()
    gcp.run_ingestion_workflow.assert_not_called()


@pytest.mark.parametrize("state", ["FAILED", "CANCELLED"])
def test_ingest_fails_when_the_execution_ends_while_watched(
    tmp_path: Path,
    gcp: MagicMock,
    knowledge_graph: KnowledgeGraphResource,
    state: str,
) -> None:
    """Test an execution that ends early fails the ingestion with its error"""
    bundle = write_bundle(tmp_path / "bundle")
    gcp.get_ingestion_execution_state.return_value = IngestionExecutionState(
        state=state, current_steps=(), error="preprocessing job failed"
    )

    with pytest.raises(IngestionExecutionError, match="preprocessing job failed"):
        ingest(bundle, knowledge_graph)


def test_watch_returns_a_succeeded_execution_without_waiting(
    gcp: MagicMock, knowledge_graph: KnowledgeGraphResource
) -> None:
    """Test a finished execution ends the watch at the first read"""
    time = FakeTime()

    execution = watch_execution_start(
        knowledge_graph.settings,
        EXECUTION_NAME,
        log=LOG,
        watch_seconds=120,
        poll_seconds=5,
        sleep=time.sleep,
        clock=time.clock,
    )

    assert execution == SUCCEEDED
    assert time.now == 0


def test_watch_leaves_an_active_execution_running_when_the_watch_ends(
    gcp: MagicMock, knowledge_graph: KnowledgeGraphResource
) -> None:
    """Test an execution still running at the deadline is returned, not failed"""
    gcp.get_ingestion_execution_state.return_value = ACTIVE
    time = FakeTime()

    execution = watch_execution_start(
        knowledge_graph.settings,
        EXECUTION_NAME,
        log=LOG,
        watch_seconds=12,
        poll_seconds=5,
        sleep=time.sleep,
        clock=time.clock,
    )

    assert execution == ACTIVE
    # Read at 0, 5, 10 and 15 seconds, the last past the 12-second deadline.
    assert gcp.get_ingestion_execution_state.call_count == 4
    assert time.now == 15


def test_watch_fails_an_execution_that_fails_after_starting(
    gcp: MagicMock, knowledge_graph: KnowledgeGraphResource
) -> None:
    """Test a failure inside the watch is caught after the execution was running"""
    gcp.get_ingestion_execution_state.side_effect = [
        ACTIVE,
        IngestionExecutionState(state="FAILED", current_steps=(), error="lock busy"),
    ]
    time = FakeTime()

    with pytest.raises(IngestionExecutionError, match="lock busy"):
        watch_execution_start(
            knowledge_graph.settings,
            EXECUTION_NAME,
            log=LOG,
            watch_seconds=120,
            poll_seconds=5,
            sleep=time.sleep,
            clock=time.clock,
        )

    assert time.now == 5


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
        ingest(bundle, knowledge_graph)

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


def test_knowledge_graph_settings_ignore_the_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    knowledge_graph: KnowledgeGraphResource,
) -> None:
    """Test the settings build from the resource alone

    KGSettings would otherwise let variables it declares override the resource,
    warn about a deprecated one, and reject every variable in a .env file it does
    not declare.
    """
    (tmp_path / ".env").write_text("DUCKDB_PATH=elsewhere.duckdb\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INGESTION_PREP_JOB_NAME", "prep-job")
    monkeypatch.setenv("GCS_INPUT_FOLDER_PATH", "elsewhere/input")
    monkeypatch.setenv("GCP_CREDENTIALS", '{"type": "service_account"}')

    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        settings = knowledge_graph.settings

    assert settings.model_dump() == {
        "local_path": Path("."),
        "gcp_project_id": knowledge_graph.project_id,
        "gcp_region": knowledge_graph.region,
        "gcp_credentials": None,
        "gcs_bucket_name": knowledge_graph.bucket_name,
        "gcs_input_folder_path": "ingestion/input",
        "gcs_output_folder_path": "",
        "ingestion_workflow_name": knowledge_graph.workflow_name,
        "ingestion_service_account": None,
        "ingestion_prep_job_name": None,
    }
