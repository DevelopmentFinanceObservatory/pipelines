"""Tests of scripts/graph_check.py"""

from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import Any

import duckdb
import httpx
import pytest

from dfo_pipelines.graph_check import run_graph_check
from scripts import graph_check
from tests.graph_check.tables import SPEC, STATVARS_SQL

URL = "https://graph.test/check"


def clean_route(url: str, body: dict[str, Any]) -> httpx.Response:
    return httpx.Response(
        200, json={"generation": "g1", "row_count": len(body["rows"]), "findings": []}
    )


def dead_route(url: str, body: dict[str, Any]) -> httpx.Response:
    raise httpx.ConnectError("refused")


@pytest.fixture
def run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Callable[[Callable[..., httpx.Response], str | None], tuple[int, Path]]:
    """Run the script against a database file holding `SPEC`'s table"""
    database = tmp_path / "build.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("create schema checked")
        connection.execute(STATVARS_SQL)
    monkeypatch.setenv("DUCKDB_PATH", str(database))
    monkeypatch.setattr(graph_check, "EXPORT_SPECS", (SPEC,))

    def run_script(post: Callable[..., httpx.Response], url: str | None):
        if url is None:
            monkeypatch.delenv("GRAPH_CHECK_URL", raising=False)
        else:
            monkeypatch.setenv("GRAPH_CHECK_URL", url)
        monkeypatch.setattr(
            graph_check, "run_graph_check", partial(run_graph_check, post=post)
        )
        report = tmp_path / "report.md"
        return graph_check.main([str(report)]), report

    return run_script


def test_report_starts_with_the_marker_and_the_verdict_is_printed(
    run, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)

    status, report = run(clean_route, URL)

    lines = report.read_text().splitlines()
    assert status == 0
    assert lines[0] == graph_check.MARKER
    assert capsys.readouterr().out.strip() == lines[1]


@pytest.mark.parametrize(
    ("post", "url"),
    [(clean_route, None), (clean_route, ""), (dead_route, URL)],
    ids=["unset", "empty", "unreachable"],
)
def test_a_check_that_cannot_run_still_writes_a_report_and_exits_zero(
    run, post: Callable[..., httpx.Response], url: str | None
) -> None:
    status, report = run(post, url)

    assert status == 0
    assert "did not run" in report.read_text()


def test_the_report_is_appended_to_the_step_summary(
    run, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    summary = tmp_path / "summary.md"
    summary.write_text("earlier\n")
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))

    _, report = run(clean_route, URL)

    written = summary.read_text()
    assert written.startswith("earlier\n")
    assert report.read_text().removeprefix(graph_check.MARKER + "\n") in written
    assert graph_check.MARKER not in written


def test_a_missing_database_setting_is_a_crash(
    run, monkeypatch: pytest.MonkeyPatch
) -> None:
    run(clean_route, URL)
    monkeypatch.delenv("DUCKDB_PATH")

    with pytest.raises(KeyError):
        graph_check.main(["unused.md"])
