from collections.abc import Iterator
from datetime import datetime, timedelta, tzinfo
from itertools import count
from unittest.mock import patch

import pytest

from dfo_pipelines.orchestration.resources import KnowledgeGraphResource
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


@pytest.fixture
def download_clock() -> Iterator[None]:
    """Stamp landing downloads from DOWNLOADED_AT on, a second apart so each gets its
    own directory, in the zone each reading names"""
    instants = (DOWNLOADED_AT + timedelta(seconds=seconds) for seconds in count())

    def now(tz: tzinfo) -> datetime:
        return next(instants).astimezone(tz)

    with patch("dfo_pipelines.orchestration.landing.datetime") as mock_datetime:
        mock_datetime.now.side_effect = now
        yield
