import pytest

from dfo_pipelines.graph_check.report import (
    FINDING_CODES,
    MAX_REPORT_CHARACTERS,
    CheckSummary,
    Finding,
    RefusedRow,
    render_report,
)

# The codes the check route documents.
ROUTE_CODES = [
    "node_not_held",
    "node_untyped",
    "node_provisional",
    "value_missing",
    "not_a_property",
    "not_a_class",
    "value_outside_range",
    "property_outside_domain",
    "definition_differs",
]


def make_finding(
    table: str = "oecd.statvars", code: str = "node_not_held", severity: str = "error"
) -> Finding:
    return Finding(
        table=table,
        statvar="Variable1",
        field="constraint_properties.crsPurpose",
        value="CRSPurposeCode99999",
        code=code,
        severity=severity,
    )


def first_line(report: str) -> str:
    return report.splitlines()[0]


@pytest.mark.parametrize(
    ("summary", "verdict"),
    [
        (
            CheckSummary(not_run="no check URL is configured"),
            "The graph check did not run: no check URL is configured.",
        ),
        (
            CheckSummary(
                statvar_count=3, tables_checked=("a", "b"), generations=("g1",)
            ),
            "No problems found in 3 StatVars across 2 tables, checked against "
            "graph generation `g1`.",
        ),
        (
            CheckSummary(
                statvar_count=1,
                tables_checked=("a",),
                unchecked_tables=(("b", "has no rows in this build"),),
                generations=(None,),
            ),
            "No problems found in 1 StatVar across 1 table, checked against the "
            "current graph (the service named no generation); 1 table was not checked.",
        ),
        (
            CheckSummary(
                statvar_count=9,
                tables_checked=("a", "b", "c"),
                tables_with_findings=2,
                error_count=3,
                warning_count=1,
                refused_count=2,
                generations=("g1", "g2"),
            ),
            "Found 3 errors and 1 warning across 2 tables, checked against graph "
            "generations `g1` and `g2` (the graph changed during the check); "
            "2 StatVars could not be checked.",
        ),
    ],
    ids=["no url", "clean", "a table not checked", "findings"],
)
def test_the_first_line_is_the_verdict(summary: CheckSummary, verdict: str) -> None:
    assert first_line(render_report(summary)) == verdict


@pytest.mark.parametrize(
    "summary",
    [
        CheckSummary(not_run="no check URL is configured"),
        CheckSummary(statvar_count=1, tables_checked=("a",), refused_count=1),
        CheckSummary(
            statvar_count=1,
            tables_checked=("a",),
            unchecked_tables=(("b", "has no rows in this build"),),
        ),
    ],
    ids=["did not run", "a StatVar refused", "a table not checked"],
)
def test_a_check_that_did_not_cover_everything_is_not_clean(
    summary: CheckSummary,
) -> None:
    assert not summary.clean


def test_findings_are_tabled_per_table_with_examples_and_explained() -> None:
    summary = CheckSummary(
        statvar_count=2,
        tables_checked=("oecd.statvars",),
        tables_with_findings=1,
        unchecked_tables=(("imf.statvars", "has no rows in this build"),),
        error_count=1,
        refused_count=1,
        generations=("g1",),
    )
    refused = RefusedRow(table="oecd.statvars", statvar="Variable2", detail="bad")

    report = render_report(summary, findings=[make_finding()], refused=[refused])

    assert (
        "| `node_not_held` | error | 1 | `Variable1` "
        "`constraint_properties.crsPurpose` = `CRSPurposeCode99999` |"
    ) in report
    assert "- `Variable2`: bad" in report
    assert (
        "- `imf.statvars` has no rows in this build, so it was not checked." in report
    )
    assert f"- `node_not_held`: {FINDING_CODES['node_not_held']}" in report


def test_every_route_code_has_an_explanation() -> None:
    assert sorted(FINDING_CODES) == sorted(ROUTE_CODES)
    assert all(FINDING_CODES.values())


def test_a_long_report_is_cut_to_the_cap_with_its_counts_kept() -> None:
    tables = [f"source{n}.statvars" for n in range(400)]
    findings = [make_finding(table) for table in tables for _ in range(3)]
    summary = CheckSummary(
        statvar_count=len(findings),
        tables_checked=tuple(tables),
        tables_with_findings=len(tables),
        error_count=len(findings),
        generations=("g1",),
    )

    report = render_report(summary, findings=findings)

    assert len(report) <= MAX_REPORT_CHARACTERS
    assert first_line(report).startswith("Found 1,200 errors and 0 warnings")
    assert "the counts are complete" in report
    assert "more tables with" in report
