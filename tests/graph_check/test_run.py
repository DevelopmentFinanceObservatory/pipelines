from dataclasses import replace
from pathlib import Path
from typing import Any

import duckdb
import httpx
import pytest
from duckdb import DuckDBPyConnection

from dfo_pipelines.graph_check.run import RepeatedStatVarModelError, run_graph_check
from tests.graph_check.tables import (
    SPEC,
    STATVARS_SQL,
    URL,
    answer,
    clean_route,
    first_line,
    spans,
)


def refuse_every_call(url: str, body: dict[str, Any], timeout: float) -> httpx.Response:
    raise AssertionError("the route was called")


def refused() -> httpx.Response:
    """The route's refusal of the rows it was sent"""
    return httpx.Response(400, json={"code": "request_invalid", "detail": "bad"})


def test_no_url_reports_that_the_check_did_not_run(
    connection: DuckDBPyConnection,
) -> None:
    report, summary = run_graph_check(
        connection, specs=[SPEC], url=None, post=refuse_every_call
    )

    assert not summary.ran
    assert report.startswith("The graph check did not run: no check URL")


def test_findings_name_their_table_and_statvar(connection: DuckDBPyConnection) -> None:
    sent: list[dict[str, Any]] = []

    def route(url: str, body: dict[str, Any], timeout: float) -> httpx.Response:
        sent.extend(body["rows"])
        return answer(
            body["rows"],
            findings=[
                {
                    "ref": "checked.statvars:Variable2",
                    "field": "constraint_properties.crsPurpose",
                    "value": None,
                    "code": "value_missing",
                    "severity": "error",
                }
            ],
        )

    report, summary = run_graph_check(connection, specs=[SPEC], url=URL, post=route)

    assert len(sent) == 2
    assert (summary.statvar_count, summary.checked_count) == (2, 2)
    assert summary.error_count == 1
    assert "### `checked.statvars`" in report
    assert "`Variable2` `constraint_properties.crsPurpose` = `null`" in report


def test_an_empty_table_is_reported_and_the_rest_are_checked(
    connection: DuckDBPyConnection,
) -> None:
    connection.execute("create table checked.empty as from checked.statvars limit 0")
    empty = replace(SPEC, statvar_model="checked.empty")

    report, summary = run_graph_check(
        connection, specs=[SPEC, empty], url=URL, post=clean_route
    )

    assert first_line(report) == (
        "Checked 2 StatVars across 1 table. 1 table was not checked."
    )
    assert summary.unchecked_tables == (("checked.empty", "has no rows in this build"),)
    assert [spans(line) for line in report.splitlines() if line.startswith("- ")] == [
        ["checked.empty"]
    ]


def test_no_rows_anywhere_means_the_check_did_not_run(
    connection: DuckDBPyConnection,
) -> None:
    missing = replace(SPEC, statvar_model="checked.missing")

    report, summary = run_graph_check(
        connection, specs=[missing], url=URL, post=refuse_every_call
    )

    assert not summary.ran
    assert summary.unchecked_tables == (("checked.missing", "is not in this build"),)
    assert first_line(report) == (
        "The graph check did not run: no StatVar table has rows in this build."
    )


def test_an_unreachable_route_means_the_check_did_not_run(
    connection: DuckDBPyConnection,
) -> None:
    def unreachable(url: str, body: dict[str, Any], timeout: float) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    report, summary = run_graph_check(
        connection, specs=[SPEC], url=URL, post=unreachable
    )

    assert not summary.ran
    assert first_line(report) == (
        "The graph check did not run: the check route could not be reached."
    )
    assert spans(report.split("\n\n")[1]) == ["ConnectError"]


def test_a_statvar_without_a_measured_property_is_refused_without_a_call(
    connection: DuckDBPyConnection,
) -> None:
    connection.execute(
        "update checked.statvars set measured_property = null"
        " where dcid = 'dcid:Variable1'"
    )
    sent: list[dict[str, Any]] = []

    def route(url: str, body: dict[str, Any], timeout: float) -> httpx.Response:
        sent.extend(body["rows"])
        return answer(body["rows"])

    report, summary = run_graph_check(connection, specs=[SPEC], url=URL, post=route)

    assert [row["dcid"] for row in sent] == ["Variable2"]
    assert (summary.unsent_count, summary.unreached_count) == (1, 0)
    assert (
        first_line(report) == "Checked 1 of 2 StatVars across 1 table. 1 was not sent."
    )
    assert [spans(line) for line in report.splitlines() if line.startswith("- ")] == [
        ["Variable1"]
    ]


def test_no_statvar_with_a_measured_property_means_the_check_did_not_run(
    connection: DuckDBPyConnection,
) -> None:
    connection.execute("update checked.statvars set measured_property = null")

    report, summary = run_graph_check(
        connection, specs=[SPEC], url=URL, post=refuse_every_call
    )

    assert not summary.ran
    assert first_line(report) == (
        "The graph check did not run: none of the 2 StatVars in this build has a "
        "measured property."
    )


def test_a_database_path_is_opened_and_read(tmp_path: Path) -> None:
    database = tmp_path / "build.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("create schema checked")
        connection.execute(STATVARS_SQL)

    report, _ = run_graph_check(database, specs=[SPEC], url=URL, post=clean_route)

    assert first_line(report) == "No problems found in 2 StatVars across 1 table."


def test_a_check_that_stops_part_way_reports_what_it_found_and_what_it_did_not(
    connection: DuckDBPyConnection,
) -> None:
    def route(url: str, body: dict[str, Any], timeout: float) -> httpx.Response:
        rows = body["rows"]
        if len(rows) > 1:
            return refused()
        if rows[0]["dcid"] == "Variable2":
            return httpx.Response(500)
        return answer(
            rows,
            findings=[
                {
                    "ref": rows[0]["ref"],
                    "field": "population_type",
                    "value": "FinancialFlow",
                    "code": "not_a_class",
                    "severity": "error",
                }
            ],
        )

    report, _ = run_graph_check(connection, specs=[SPEC], url=URL, post=route)

    assert first_line(report) == (
        "Found 1 error in 1 of 2 StatVars checked across 1 table. "
        "1 was not reached because the check route failed."
    )
    assert spans(report.split("\n\n")[1]) == ["answer 500, no detail given", "g1"]
    assert "`Variable1` `population_type` = `FinancialFlow`" in report


def test_a_check_whose_every_sent_statvar_was_refused_before_it_stopped_ran(
    connection: DuckDBPyConnection,
) -> None:
    def route(url: str, body: dict[str, Any], timeout: float) -> httpx.Response:
        if body["rows"][0]["dcid"] == "Variable1":
            return refused()
        return httpx.Response(500)

    _, summary = run_graph_check(connection, specs=[SPEC], url=URL, post=route)

    assert summary.ran
    assert (summary.checked_count, summary.route_refused_count) == (0, 1)
    assert summary.unreached_count == 1


def test_specs_sharing_a_statvar_model_are_refused(
    connection: DuckDBPyConnection,
) -> None:
    with pytest.raises(RepeatedStatVarModelError) as raised:
        run_graph_check(connection, specs=[SPEC, SPEC], url=URL, post=clean_route)

    assert raised.value.models == (SPEC.statvar_model,)
