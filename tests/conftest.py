from collections.abc import Iterator
from datetime import datetime, tzinfo
from pathlib import Path
from unittest.mock import PropertyMock, patch
from uuid import uuid4

import dagster as dg
import pytest
from dagster._core.execution.context.invocation import DirectOpExecutionContext
from upath import UPath

from dfo_pipelines.resources.knowledge_graph import KnowledgeGraphResource
from tests.helpers import DOWNLOADED_AT


@pytest.fixture
def knowledge_graph() -> KnowledgeGraphResource:
    """Knowledge graph a test hands an ingestion, never reached"""
    return KnowledgeGraphResource(
        project_id="test-project",
        region="test-region",
        bucket_name="test-bucket",
        workflow_name="test-workflow",
    )


@pytest.fixture(scope="session", autouse=True)
def local_environment() -> Iterator[None]:
    """Load definitions for the local environment, whatever the shell holds"""
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setenv("DFO_ENVIRONMENT", "local")
        yield


@pytest.fixture(autouse=True)
def direct_invocation_run() -> Iterator[None]:
    """Give a directly invoked asset a run, as `build_asset_context` sets none"""
    with patch.object(
        DirectOpExecutionContext,
        "dagster_run",
        new_callable=PropertyMock,
        return_value=dg.DagsterRun(job_name="direct", run_id="direct-run"),
    ):
        yield


@pytest.fixture
def download_clock() -> Iterator[None]:
    """Stamp landing downloads with DOWNLOADED_AT, in the zone each reading names"""

    def now(tz: tzinfo) -> datetime:
        return DOWNLOADED_AT.astimezone(tz)

    with patch("dfo_pipelines.orchestration.landing.datetime") as mock_datetime:
        mock_datetime.now.side_effect = now
        yield


@pytest.fixture(params=["local", "memory"])
def landing_directory(request: pytest.FixtureRequest, tmp_path: Path) -> UPath:
    """A fetch's landing directory, on local disk and on an object store"""
    if request.param == "local":
        return UPath(tmp_path)
    return UPath(f"memory://{uuid4().hex}/landing")
