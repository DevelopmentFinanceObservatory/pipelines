from dataclasses import replace
from pathlib import Path
from typing import Any

import duckdb
import httpx
import pytest
from duckdb import DuckDBPyConnection

from dfo_pipelines.data_commons.export import ExportSpec
from dfo_pipelines.graph_check.run import RepeatedStatVarModelError, run_graph_check
from tests.graph_check.tables import (
    SPEC,
    STATVARS_SQL,
    URL,
    answer,
    clean_route,
    finding,
    first_line,
    refused,
    spans,
)


def refuse_every_call(url: str, body: dict[str, Any], timeout: float) -> httpx.Response:
    raise AssertionError("the route was called")


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
                finding(
                    1,
                    field="constraint_properties",
                    property="crsPurpose",
                    value=None,
                    code="value_missing",
                )
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
    assert [line for line in report.splitlines() if line.startswith("- ")] == [
        "- `checked.missing` is not in this build, so it was not checked."
    ]


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


def test_a_statvar_without_a_measured_property_is_sent_for_the_route_to_report(
    connection: DuckDBPyConnection,
) -> None:
    connection.execute(
        "update checked.statvars set measured_property = null"
        " where dcid = 'dcid:Variable1'"
    )

    sent = sent_rows(connection)

    assert [(row["dcid"], row["measured_property"]) for row in sent] == [
        ("Variable1", None),
        ("Variable2", "amount"),
    ]


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
        if rows[0]["dcid"] == "Variable2":
            return httpx.Response(500)
        return answer(
            rows,
            row_count=1,
            findings=[
                finding(
                    0,
                    field="population_type",
                    value="FinancialFlow",
                    code="not_a_class",
                )
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
            return refused(0, "a string past its bound")
        return httpx.Response(500)

    report, summary = run_graph_check(connection, specs=[SPEC], url=URL, post=route)

    assert summary.ran
    assert (summary.checked_count, summary.route_refused_count) == (0, 1)
    assert summary.unreached_count == 1
    assert [spans(line) for line in report.splitlines() if line.startswith("- ")] == [
        ["Variable1", "a string past its bound"],
        ["checked.statvars"],
    ]


def test_specs_sharing_a_statvar_model_are_refused(
    connection: DuckDBPyConnection,
) -> None:
    with pytest.raises(RepeatedStatVarModelError) as raised:
        run_graph_check(connection, specs=[SPEC, SPEC], url=URL, post=clean_route)

    assert raised.value.models == (SPEC.statvar_model,)


def sent_rows(
    connection: DuckDBPyConnection, specs: list[ExportSpec] | None = None
) -> list[dict[str, Any]]:
    """The rows a clean run sends to the route"""
    sent: list[dict[str, Any]] = []

    def route(url: str, body: dict[str, Any], timeout: float) -> httpx.Response:
        sent.extend(body["rows"])
        return answer(body["rows"])

    run_graph_check(connection, specs=specs or [SPEC], url=URL, post=route)
    return sent


def test_rows_go_in_the_order_the_export_writes_them(
    connection: DuckDBPyConnection,
) -> None:
    connection.execute(
        "insert into checked.statvars select * replace ('dcid:Variable0' as dcid)"
        " from checked.statvars where dcid = 'dcid:Variable1'"
    )

    sent = sent_rows(connection)

    assert [row["dcid"] for row in sent] == ["Variable0", "Variable1", "Variable2"]


@pytest.mark.parametrize(
    ("statement", "change", "member", "value"),
    [
        (
            "select 1",
            {"statvar_ignore_columns": ("measurement_qualifier",)},
            "measurement_qualifier",
            None,
        ),
        (
            "alter table checked.statvars add column unit varchar default 'dcid:Real'",
            {
                "statvar_column_to_property": SPEC.statvar_column_to_property
                | {"unit": "measurementQualifier"},
                "statvar_ignore_columns": ("measurement_qualifier",),
            },
            "measurement_qualifier",
            "Real",
        ),
        (
            "update checked.statvars set stat_type = ''",
            {},
            "stat_type",
            "measuredValue",
        ),
        (
            "update checked.statvars set measurement_qualifier = 'NA'",
            {},
            "measurement_qualifier",
            None,
        ),
    ],
    ids=[
        "ignored fixed column",
        "fixed property from a mapped column",
        "empty stat type",
        "NA token",
    ],
)
def test_fixed_members_carry_what_the_export_publishes(
    connection: DuckDBPyConnection,
    statement: str,
    change: dict[str, Any],
    member: str,
    value: str | None,
) -> None:
    connection.execute(statement)

    sent = sent_rows(connection, [replace(SPEC, **change)])

    assert [row[member] for row in sent] == [value, value]


def test_a_null_token_in_a_constraint_column_is_a_missing_value(
    connection: DuckDBPyConnection,
) -> None:
    connection.execute("update checked.statvars set netting_basis = 'null'")

    sent = sent_rows(connection)

    assert [row["constraint_properties"]["nettingBasis"] for row in sent] == [
        None,
        None,
    ]


def test_a_table_the_export_cannot_read_is_reported_and_the_rest_are_checked(
    connection: DuckDBPyConnection,
) -> None:
    connection.execute(
        "create table checked.bare_dcids as select * replace"
        " ('FinancialFlow' as population_type) from checked.statvars"
    )
    unreadable = replace(SPEC, statvar_model="checked.bare_dcids")

    _, summary = run_graph_check(
        connection, specs=[SPEC, unreadable], url=URL, post=clean_route
    )

    assert summary.tables_checked == ("checked.statvars",)
    assert summary.unchecked_tables == (
        ("checked.bare_dcids", "has StatVars the export cannot read"),
    )


def second_table(connection: DuckDBPyConnection) -> ExportSpec:
    """A copy of `SPEC`'s table whose StatVars are named `Second1` and `Second2`"""
    connection.execute(
        "create table checked.second as select * replace ("
        " replace(dcid, 'Variable', 'Second') as dcid) from checked.statvars"
    )
    return replace(SPEC, statvar_model="checked.second")


def test_a_table_the_check_stopped_before_is_not_counted_as_checked(
    connection: DuckDBPyConnection,
) -> None:
    second = second_table(connection)

    def route(url: str, body: dict[str, Any], timeout: float) -> httpx.Response:
        # The route checks the first table's rows, then fails.
        rows = body["rows"]
        if rows[0]["dcid"].startswith("Second"):
            return httpx.Response(500)
        return answer(rows, row_count=2)

    report, summary = run_graph_check(
        connection, specs=[SPEC, second], url=URL, post=route
    )

    assert summary.tables_checked == ("checked.statvars",)
    assert summary.unchecked_tables == (
        ("checked.second", "was not reached before the check stopped"),
    )
    assert first_line(report) == (
        "Checked 2 of 4 StatVars across 1 table. 2 were not reached because the "
        "check route failed. 1 table was not checked."
    )


def test_a_table_whose_every_statvar_was_refused_is_not_counted_as_checked(
    connection: DuckDBPyConnection,
) -> None:
    second = second_table(connection)

    def route(url: str, body: dict[str, Any], timeout: float) -> httpx.Response:
        dcids = [row["dcid"] for row in body["rows"]]
        if seconds := [n for n, dcid in enumerate(dcids) if dcid.startswith("Second")]:
            return refused(seconds[0])
        return answer(body["rows"])

    _, summary = run_graph_check(connection, specs=[SPEC, second], url=URL, post=route)

    assert summary.tables_checked == ("checked.statvars",)
    assert summary.route_refused_count == 2
    assert summary.unchecked_tables == (
        ("checked.second", "has no StatVar the check route could check"),
    )
