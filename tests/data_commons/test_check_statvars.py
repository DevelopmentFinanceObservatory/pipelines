"""Tests of scripts/check_statvars.py."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import duckdb
import httpx
import pytest

from dfo_pipelines.data_commons.graph_check import (
    DEFAULT_GRAPH_API_URL,
    GRAPH_API_URL_VARIABLE,
)
from scripts import check_statvars

FINDING: dict[str, Any] = {
    "row": 0,
    "field": "stat_type",
    "property": None,
    "subject": "value",
    "value": "measuredValue",
    "served": None,
    "code": "node_not_held",
    "severity": "error",
}


def checked(*findings: dict[str, Any]) -> httpx.Response:
    return httpx.Response(
        200, json={"generation": "g1", "rows_checked": 1, "findings": list(findings)}
    )


def unreachable(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("refused")


def fails(request: httpx.Request) -> httpx.Response:
    raise RuntimeError("a fault the check does not expect")


def refused(request: httpx.Request) -> httpx.Response:
    return httpx.Response(
        400, json={"detail": "not finite", "pointer": "/rows/0/stat_type"}
    )


@pytest.fixture
def database(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A database holding one StatVar in the DAC2A export's StatVar model"""
    path = tmp_path / "database.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute("create schema oecd_dac2a")
        connection.execute(
            "create table oecd_dac2a.dc_dac2a_statvars as select"
            " 'dcid:Variable1' as dcid, 'dcid:measuredValue' as stat_type"
        )
    monkeypatch.setenv("DUCKDB_PATH", str(path))


@pytest.mark.parametrize(
    ("answer", "exit_code"),
    [
        pytest.param(lambda request: checked(), check_statvars.EXIT_CLEAN, id="clean"),
        pytest.param(
            lambda request: checked(FINDING),
            check_statvars.EXIT_FINDINGS,
            id="findings",
        ),
        pytest.param(
            lambda request: checked(
                FINDING | {"code": "definition_differs", "severity": "warning"}
            ),
            check_statvars.EXIT_CLEAN,
            id="warnings only",
        ),
        pytest.param(refused, check_statvars.EXIT_FINDINGS, id="refused row"),
        pytest.param(unreachable, check_statvars.EXIT_UNCHECKED, id="unreachable"),
        pytest.param(fails, check_statvars.EXIT_UNCHECKED, id="unexpected error"),
        pytest.param(
            lambda request: httpx.Response(200, text="<html>"),
            check_statvars.EXIT_UNCHECKED,
            id="200 not JSON",
        ),
        pytest.param(
            lambda request: httpx.Response(503, headers={"Retry-After": "600"}),
            check_statvars.EXIT_UNCHECKED,
            id="past the wait limit",
        ),
    ],
)
def test_main_exits_by_what_the_check_found(
    database: None,
    monkeypatch: pytest.MonkeyPatch,
    answer: Callable[[httpx.Request], httpx.Response],
    exit_code: int,
) -> None:
    """Test the command exits 0 on a check without errors, 1 on errors or a refused
    row, and 2 when the check could not finish"""
    # The trailing newline a `.env` value can carry is stripped.
    monkeypatch.setenv(GRAPH_API_URL_VARIABLE, "https://graph.example\n")
    monkeypatch.setattr(
        "dfo_pipelines.data_commons.graph_check.open_client",
        lambda base_url: httpx.Client(
            base_url=base_url, transport=httpx.MockTransport(answer)
        ),
    )

    assert check_statvars.main(["oecd_dac2a"]) == exit_code


@pytest.mark.parametrize("value", [None, "", "  "], ids=["unset", "empty", "blank"])
def test_main_checks_the_default_graph_api_when_the_variable_is_unset_or_empty(
    database: None, monkeypatch: pytest.MonkeyPatch, value: str | None
) -> None:
    """Test the command posts to the public Graph API when the variable has no
    value"""
    if value is None:
        monkeypatch.delenv(GRAPH_API_URL_VARIABLE, raising=False)
    else:
        monkeypatch.setenv(GRAPH_API_URL_VARIABLE, value)
    urls: list[str] = []

    def open_client(base_url: str) -> httpx.Client:
        urls.append(base_url)
        return httpx.Client(
            base_url=base_url,
            transport=httpx.MockTransport(lambda request: checked()),
        )

    monkeypatch.setattr(
        "dfo_pipelines.data_commons.graph_check.open_client", open_client
    )

    assert check_statvars.main(["oecd_dac2a"]) == check_statvars.EXIT_CLEAN
    assert urls == [DEFAULT_GRAPH_API_URL]


def test_main_exits_unchecked_when_duckdb_path_is_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test the command exits unchecked when it has no database to read"""
    monkeypatch.setenv(GRAPH_API_URL_VARIABLE, "https://graph.example")
    monkeypatch.delenv("DUCKDB_PATH", raising=False)

    assert check_statvars.main(["oecd_dac2a"]) == check_statvars.EXIT_UNCHECKED


def test_main_exits_unchecked_when_the_model_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test the command exits unchecked when the StatVar model is not built"""
    path = tmp_path / "database.duckdb"
    duckdb.connect(str(path)).close()
    monkeypatch.setenv(GRAPH_API_URL_VARIABLE, "https://graph.example")
    monkeypatch.setenv("DUCKDB_PATH", str(path))

    assert check_statvars.main(["oecd_dac2a"]) == check_statvars.EXIT_UNCHECKED
