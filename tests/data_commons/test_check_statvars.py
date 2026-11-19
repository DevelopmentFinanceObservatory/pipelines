"""Tests of scripts/check_statvars.py."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import duckdb
import httpx
import pytest

from dfo_pipelines.data_commons.graph_check import GRAPH_API_URL_VARIABLE
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
        pytest.param(unreachable, check_statvars.EXIT_UNCHECKED, id="unreachable"),
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
    """Test the command exits 0 on a clean check, 1 on findings, and 2 when the
    check could not finish"""
    monkeypatch.setenv(GRAPH_API_URL_VARIABLE, "https://graph.example")
    monkeypatch.setattr(
        check_statvars,
        "open_client",
        lambda base_url: httpx.Client(
            base_url=base_url, transport=httpx.MockTransport(answer)
        ),
    )

    assert check_statvars.main(["oecd_dac2a"]) == exit_code


def test_main_exits_unchecked_when_the_graph_api_is_unset(
    database: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test the command refuses to report a clean check it never ran"""
    monkeypatch.delenv(GRAPH_API_URL_VARIABLE, raising=False)

    assert check_statvars.main(["oecd_dac2a"]) == check_statvars.EXIT_UNCHECKED
