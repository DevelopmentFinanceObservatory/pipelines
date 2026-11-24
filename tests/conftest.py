from collections.abc import Iterator
from datetime import UTC, datetime

import pytest

from dfo_pipelines.orchestration.resources import KnowledgeGraphResource
from tests.helpers import stepped_download_clock


@pytest.fixture
def knowledge_graph() -> KnowledgeGraphResource:
    """Knowledge graph a test hands an ingestion, never reached"""
    return KnowledgeGraphResource(
        project_id="test-project",
        region="test-region",
        bucket_name="test-bucket",
        workflow_name="test-workflow",
    )


@pytest.fixture
def download_clock() -> Iterator[None]:
    """Stamp each download a second after the last, so a test landing twice gets two
    directories"""
    with stepped_download_clock(datetime(2026, 8, 25, 12, 0, tzinfo=UTC)):
        yield
