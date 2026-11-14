"""Check the StatVar tables of a build against the graph and report the result."""

import time
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
from duckdb import DuckDBPyConnection

from dfo_pipelines.data_commons.export import ExportSpec
from dfo_pipelines.graph_check.client import Post, check_rows
from dfo_pipelines.graph_check.report import (
    CheckSummary,
    Finding,
    RefusedRow,
    render_report,
)
from dfo_pipelines.graph_check.rows import (
    CheckRow,
    StatVarReadError,
    read_statvars,
    statvar_check_rows,
)


class RepeatedStatVarModelError(ValueError):
    """Two export specs name the same StatVar model, whose rows would count twice."""

    def __init__(self, models: Sequence[str]) -> None:
        super().__init__(
            f"StatVar models named by more than one spec: {', '.join(models)}"
        )
        self.models = tuple(models)


def run_graph_check(
    database: DuckDBPyConnection | str | Path,
    *,
    specs: Sequence[ExportSpec],
    url: str | None,
    post: Post | None = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    commit: str | None = None,
    base_branch: str | None = None,
) -> tuple[str, CheckSummary]:
    """Check every spec's StatVar table against the graph.

    A finding is a result of the check. A check that cannot run, or stops
    before every StatVar is checked, says why in its report and keeps what it
    found, so this raises only for a fault in the specs or the database itself.

    Args:
        database: A connection to the build's database, or the path of its file,
            opened read-only.
        specs: The exports whose StatVar tables are checked.
        url: The check route's URL; None or empty when none is configured.
        post: Sends one request to the route. Defaults to one httpx client for
            the whole run.
        sleep: Waits the given seconds between retries.
        clock: Reads the time, for the run's time budget.
        commit: The pull request head commit, which the report names.
        base_branch: The branch the head commit was merged into for the build,
            which the report names beside the commit.

    Returns:
        The Markdown report and its summary.

    Raises:
        RepeatedStatVarModelError: Two specs name the same StatVar model.
    """
    models = Counter(spec.statvar_model for spec in specs)
    if repeated := [model for model, count in models.items() if count > 1]:
        raise RepeatedStatVarModelError(repeated)

    if not url:
        summary = CheckSummary(
            not_run="no check URL is configured",
            commit=commit,
            base_branch=base_branch,
        )
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
            commit=commit,
            base_branch=base_branch,
        )
        return render_report(summary), summary

    if not tables.rows:
        summary = CheckSummary(
            not_run=(
                f"none of the {len(tables.unsent):,} StatVars in this build "
                "has a measured property"
            ),
            unchecked_tables=tables.unchecked,
            commit=commit,
            base_branch=base_branch,
        )
        return render_report(summary), summary

    result = check_rows(tables.rows, url=url, post=post, sleep=sleep, clock=clock)
    stop = result.stopped
    if stop is not None and result.checked_count == 0 and not result.failed_rows:
        summary = CheckSummary(
            not_run=stop.message,
            failure_detail=stop.detail,
            unchecked_tables=tables.unchecked,
            commit=commit,
            base_branch=base_branch,
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
    settled_refs = (
        set(result.checked_refs)
        | {row.ref for row in result.failed_rows}
        | tables.unsent_refs
    )
    tables_checked = tuple(
        dict.fromkeys(tables.statvars[ref][0] for ref in result.checked_refs)
    )
    not_reached = tuple(
        (table, _unchecked_reason(table, tables, settled_refs))
        for table in tables.read
        if table not in tables_checked
    )
    summary = CheckSummary(
        stopped=None if stop is None else stop.message,
        failure_detail=None if stop is None else stop.detail,
        statvar_count=len(tables.statvars),
        checked_count=result.checked_count,
        route_refused_count=len(result.failed_rows),
        unsent_count=len(tables.unsent),
        tables_checked=tables_checked,
        unchecked_tables=tables.unchecked + not_reached,
        error_count=sum(finding.severity == "error" for finding in findings),
        warning_count=sum(finding.severity == "warning" for finding in findings),
        generations=tuple(result.generations),
        commit=commit,
        base_branch=base_branch,
    )
    return render_report(summary, findings=findings, refused=refused), summary


@dataclass(kw_only=True)
class _Tables:
    """The check rows of every table read, and what could not be sent.

    Attributes:
        rows: The rows to send.
        statvars: The table and StatVar dcid of every row's `ref`.
        read: The tables with rows, in spec order.
        unsent: The StatVars not sent, for want of a measured property, which
            the route requires of every row.
        unsent_refs: The `ref` of each StatVar in `unsent`.
        unchecked: Each table with no rows to check, with the reason.
    """

    rows: list[CheckRow] = field(default_factory=list)
    statvars: dict[str, tuple[str, str]] = field(default_factory=dict)
    read: list[str] = field(default_factory=list)
    unsent: list[RefusedRow] = field(default_factory=list)
    unsent_refs: set[str] = field(default_factory=set)
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
            statvars = read_statvars(connection, spec)
        except duckdb.CatalogException:
            unchecked.append((table, "is not in this build"))
            continue
        except StatVarReadError:
            unchecked.append((table, "has StatVars the export cannot read"))
            continue
        rows = statvar_check_rows(spec, statvars)
        if not rows:
            unchecked.append((table, "has no rows in this build"))
            continue
        tables.read.append(table)
        for row in rows:
            statvar = row["dcid"] or ""
            tables.statvars[row["ref"]] = (table, statvar)
            if row["measured_property"] is None:
                tables.unsent.append(
                    RefusedRow(table=table, statvar=statvar, by_route=False)
                )
                tables.unsent_refs.add(row["ref"])
            else:
                tables.rows.append(row)
    tables.unchecked = tuple(unchecked)
    return tables


def _unchecked_reason(table: str, tables: _Tables, settled_refs: set[str]) -> str:
    """Why a table with rows has none the route answered.

    `settled_refs` holds every row answered, refused by the route, or not sent.
    """
    refs = (ref for ref, (owner, _) in tables.statvars.items() if owner == table)
    if any(ref not in settled_refs for ref in refs):
        return "was not reached before the check stopped"
    return "has no StatVar the check route could check"
