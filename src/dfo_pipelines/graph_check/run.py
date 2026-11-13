"""Check the StatVar tables of a build against the graph and report the result."""

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
from duckdb import DuckDBPyConnection

from dfo_pipelines.data_commons.export import ExportSpec, get_model_rows
from dfo_pipelines.graph_check.client import Post, check_rows, post_json
from dfo_pipelines.graph_check.report import (
    CheckSummary,
    Finding,
    RefusedRow,
    render_report,
)
from dfo_pipelines.graph_check.rows import CheckRow, statvar_check_rows


def run_graph_check(
    database: DuckDBPyConnection | str | Path,
    *,
    specs: Sequence[ExportSpec],
    url: str | None,
    post: Post = post_json,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> tuple[str, CheckSummary]:
    """Check every spec's StatVar table against the graph.

    A finding is a result, not a failure. A check that cannot run, or stops
    before every StatVar is checked, says why in its report and keeps what it
    found, so this raises only for a fault in the database itself.

    Args:
        database: A connection to the build's database, or the path of its file,
            opened read-only.
        specs: The exports whose StatVar tables are checked.
        url: The check route's URL; None or empty when none is configured.
        post: Sends one request to the route.
        sleep: Waits the given seconds between retries.
        clock: Reads the time, for the run's time budget.

    Returns:
        The Markdown report and its summary.
    """
    if not url:
        summary = CheckSummary(not_run="no check URL is configured")
        return render_report(summary), summary

    if isinstance(database, DuckDBPyConnection):
        tables = _read_tables(database, specs)
    else:
        with duckdb.connect(str(database), read_only=True) as connection:
            tables = _read_tables(connection, specs)

    if not tables.statvars:
        summary = CheckSummary(
            not_run="no StatVar table has rows in this build",
            unchecked_tables=tables.unchecked,
        )
        return render_report(summary), summary

    if not tables.rows:
        summary = CheckSummary(
            not_run=(
                f"none of the {len(tables.unsent):,} StatVars in this build "
                "has a measured property"
            ),
            unchecked_tables=tables.unchecked,
        )
        return render_report(summary), summary

    result = check_rows(tables.rows, url=url, post=post, sleep=sleep, clock=clock)
    stop = result.stopped
    if stop is not None and result.checked_count == 0 and not result.failed_rows:
        summary = CheckSummary(
            not_run=str(stop.reason),
            failure_detail=stop.detail,
            unchecked_tables=tables.unchecked,
        )
        return render_report(summary), summary

    findings = []
    for finding in result.findings:
        table, statvar = tables.statvars[finding["ref"]]
        findings.append(
            Finding(
                table=table,
                statvar=statvar,
                field=finding["field"],
                value=finding["value"],
                code=finding["code"],
                severity=finding["severity"],
            )
        )
    refused = tables.unsent + [
        RefusedRow(
            table=tables.statvars[row.ref][0],
            statvar=tables.statvars[row.ref][1],
            by_route=True,
            detail=row.detail,
        )
        for row in result.failed_rows
    ]
    summary = CheckSummary(
        stopped=None if stop is None else str(stop.reason),
        failure_detail=None if stop is None else stop.detail,
        statvar_count=len(tables.statvars),
        checked_count=result.checked_count,
        route_refused_count=len(result.failed_rows),
        unsent_count=len(tables.unsent),
        tables_checked=tuple(dict.fromkeys(t for t, _ in tables.statvars.values())),
        tables_with_findings=len(
            {finding.table for finding in findings} | {row.table for row in refused}
        ),
        unchecked_tables=tables.unchecked,
        error_count=sum(finding.severity == "error" for finding in findings),
        warning_count=sum(finding.severity == "warning" for finding in findings),
        generations=tuple(result.generations),
    )
    return render_report(summary, findings=findings, refused=refused), summary


@dataclass(kw_only=True)
class _Tables:
    """The check rows of every table read, and what could not be sent.

    Attributes:
        rows: The rows to send.
        statvars: The table and StatVar dcid of every row's `ref`.
        unsent: The StatVars not sent, for want of a measured property, which
            the route requires of every row.
        unchecked: Each table with no rows to check, with the reason.
    """

    rows: list[CheckRow] = field(default_factory=list)
    statvars: dict[str, tuple[str, str]] = field(default_factory=dict)
    unsent: list[RefusedRow] = field(default_factory=list)
    unchecked: tuple[tuple[str, str], ...] = ()


def _read_tables(
    connection: DuckDBPyConnection, specs: Sequence[ExportSpec]
) -> _Tables:
    """Read each spec's StatVar table into check rows"""
    tables = _Tables()
    unchecked = []
    for spec in specs:
        table = spec.statvar_model
        try:
            statvars = get_model_rows(connection, table)
        except duckdb.CatalogException:
            unchecked.append((table, "is not in this build"))
            continue
        rows = statvar_check_rows(spec, statvars)
        if not rows:
            unchecked.append((table, "has no rows in this build"))
            continue
        for row in rows:
            statvar = row["dcid"] or ""
            tables.statvars[row["ref"]] = (table, statvar)
            if row["measured_property"] is None:
                tables.unsent.append(
                    RefusedRow(table=table, statvar=statvar, by_route=False)
                )
            else:
                tables.rows.append(row)
    tables.unchecked = tuple(unchecked)
    return tables
