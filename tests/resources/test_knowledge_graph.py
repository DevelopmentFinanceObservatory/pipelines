from pathlib import Path

import pytest

from dfo_pipelines.resources.knowledge_graph import KnowledgeGraphResource

# KGSettings fields the resource leaves to dcp-tools' defaults
DEFAULTED_SETTINGS = {
    "LOCAL_PATH": "/elsewhere",
    "GCP_CREDENTIALS": '{"type": "service_account"}',
    "GCS_INPUT_FOLDER_PATH": "elsewhere/input",
    "GCS_OUTPUT_FOLDER_PATH": "elsewhere/output",
}


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
