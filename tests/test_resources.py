from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from dfo_pipelines.defs.resources import KnowledgeGraphResource, UPathFileStoreResource

# KGSettings fields the resource leaves to dcp-tools' defaults
DEFAULTED_SETTINGS = {
    "LOCAL_PATH": "/elsewhere",
    "GCP_CREDENTIALS": '{"type": "service_account"}',
    "GCS_INPUT_FOLDER_PATH": "elsewhere/input",
    "GCS_OUTPUT_FOLDER_PATH": "elsewhere/output",
}


def test_upath_file_store_build_date_partitioned_path(tmp_path: Path) -> None:
    """Test the built path joins the base path, the given path and the date"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path))
    path = file_store.build_date_partitioned_path(
        "oecd_dac_crs/dac_crs", partition_date=date(2026, 8, 3)
    )
    assert str(path) == f"{tmp_path}/oecd_dac_crs/dac_crs/2026/08/03"


def test_upath_file_store_build_date_partitioned_path_defaults_to_today(
    tmp_path: Path,
) -> None:
    """Test the partition defaults to today when no date is given"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path))
    with patch("dfo_pipelines.defs.resources.date") as mock_date:
        mock_date.today.return_value = date(2026, 8, 3)
        path = file_store.build_date_partitioned_path("oecd_dac_crs/dac_crs")

    assert str(path) == f"{tmp_path}/oecd_dac_crs/dac_crs/2026/08/03"


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
