import pytest

from dfo_pipelines.data_commons.graph_check import GRAPH_API_URL_VARIABLE
from dfo_pipelines.orchestration.resources import KnowledgeGraphResource


@pytest.fixture(autouse=True)
def no_graph_api(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep every export's graph check from reaching a deployment the shell names"""
    monkeypatch.delenv(GRAPH_API_URL_VARIABLE, raising=False)


@pytest.fixture
def knowledge_graph() -> KnowledgeGraphResource:
    """Knowledge graph a test hands an ingestion, never reached"""
    return KnowledgeGraphResource(
        project_id="test-project",
        region="test-region",
        bucket_name="test-bucket",
        workflow_name="test-workflow",
    )
