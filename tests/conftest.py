import pytest

from dfo_pipelines.defs.resources import KnowledgeGraphResource


@pytest.fixture
def knowledge_graph() -> KnowledgeGraphResource:
    """Knowledge graph a test hands an ingestion, never reached"""
    return KnowledgeGraphResource(
        project_id="test-project",
        region="test-region",
        bucket_name="test-bucket",
        workflow_name="test-workflow",
    )
