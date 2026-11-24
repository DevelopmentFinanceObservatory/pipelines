import logging
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import dagster as dg
import pytest
from pydantic import ValidationError

from dfo_pipelines.orchestration.resources import (
    KnowledgeGraphResource,
    UPathFileStoreResource,
)
from tests.helpers import project_definitions

# KGSettings fields the resource leaves to dcp-tools' defaults
DEFAULTED_SETTINGS = {
    "LOCAL_PATH": "/elsewhere",
    "GCP_CREDENTIALS": '{"type": "service_account"}',
    "GCS_INPUT_FOLDER_PATH": "elsewhere/input",
    "GCS_OUTPUT_FOLDER_PATH": "elsewhere/output",
}


def test_upath_file_store_names_a_write_once_directory_by_its_utc_instant_and_run(
    tmp_path: Path,
) -> None:
    """Test a directory created at an instant given in another zone is named by
    that instant in UTC, then the run"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path))
    created_at = datetime(2026, 8, 4, 1, 47, 5, tzinfo=timezone(timedelta(hours=2)))

    with file_store.write_once_directory(
        "oecd_dac1/dac1",
        created_at=created_at,
        run_id="run-a",
        log=logging.getLogger(__name__),
    ) as directory:
        pass

    assert str(directory) == f"{tmp_path}/oecd_dac1/dac1/20260803T234705Z_run-a"


def test_upath_file_store_writes_runs_in_the_same_second_to_separate_directories(
    tmp_path: Path,
) -> None:
    """Test two runs at one instant get different directories, and the second leaves
    the first's files alone"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path))
    created_at = datetime(2026, 8, 3, 23, 47, 5, tzinfo=UTC)
    log = logging.getLogger(__name__)

    with file_store.write_once_directory(
        "dac1", created_at=created_at, run_id="run-a", log=log
    ) as first:
        (first / "earlier.txt").write_text("earlier")
    with file_store.write_once_directory(
        "dac1", created_at=created_at, run_id="run-b", log=log
    ) as second:
        pass

    assert first != second
    assert [path.name for path in first.iterdir()] == ["earlier.txt"]


@pytest.mark.parametrize("source", ["environment", ".env"])
def test_knowledge_graph_settings_ignore_environment(
    source: str,
    knowledge_graph: KnowledgeGraphResource,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test the environment and .env cannot set the fields left to dcp-tools' defaults"""
    monkeypatch.chdir(tmp_path)
    for name in DEFAULTED_SETTINGS:
        monkeypatch.delenv(name, raising=False)
    expected = knowledge_graph.settings

    if source == "environment":
        for name, value in DEFAULTED_SETTINGS.items():
            monkeypatch.setenv(name, value)
    else:
        (tmp_path / ".env").write_text(
            "".join(f"{name}={value}\n" for name, value in DEFAULTED_SETTINGS.items())
        )

    assert knowledge_graph.settings == expected


def _rejects_undeclared_keyword(resource_type: type[dg.ConfigurableResource]) -> bool:
    """Whether constructing the resource with an undeclared keyword argument raises"""
    try:
        resource_type(undeclared="value")
    except ValidationError as error:
        return any(detail["type"] == "extra_forbidden" for detail in error.errors())
    return False


def test_project_resources_reject_undeclared_keyword_arguments() -> None:
    """Test every resource class the project defines raises on an undeclared keyword"""
    project_resource_types = {
        key: type(resource)
        for key, resource in (project_definitions().resources or {}).items()
        if type(resource).__module__.startswith("dfo_pipelines")
    }

    assert project_resource_types
    assert [
        key
        for key, resource_type in project_resource_types.items()
        if not _rejects_undeclared_keyword(resource_type)
    ] == []
