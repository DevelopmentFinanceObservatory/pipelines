import pytest

from dfo_pipelines.graph_check.report import (
    ADVISORY_LINE,
    FINDING_CODES,
    MAX_REPORT_CHARACTERS,
    MAX_VALUE_CHARACTERS,
    SEVERITY_LINE,
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
                statvar_count=3,
                checked_count=3,
                tables_checked=("a", "b"),
                generations=("g1",),
            ),
            "No problems found in 3 StatVars across 2 tables.",
        ),
        (
            CheckSummary(
                statvar_count=1,
                checked_count=1,
                tables_checked=("a",),
                unchecked_tables=(("b", "has no rows in this build"),),
                generations=(None,),
            ),
            "Checked 1 StatVar across 1 table; 1 table not checked.",
        ),
        (
            CheckSummary(
                statvar_count=2,
                checked_count=0,
                route_refused_count=2,
                tables_checked=("a",),
                tables_with_findings=1,
            ),
            "Checked 0 of 2 StatVars across 1 table; 2 refused by the check route.",
        ),
        (
            CheckSummary(
                statvar_count=12,
                checked_count=9,
                route_refused_count=2,
                unsent_count=1,
                tables_checked=("a", "b", "c"),
                tables_with_findings=2,
                error_count=3,
                warning_count=1,
                generations=("g1", "g2"),
            ),
            "Found 3 errors and 1 warning in 9 of 12 StatVars checked across 3 "
            "tables; 2 refused by the check route; 1 not sent.",
        ),
        (
            CheckSummary(
                stopped="the check route stayed busy",
                failure_detail="still answering 429 after 60 seconds",
                statvar_count=22_000,
                checked_count=1_200,
                tables_checked=("a",),
                tables_with_findings=1,
                warning_count=1,
            ),
            "Found 1 warning in 1,200 of 22,000 StatVars checked across 1 table; "
            "20,800 not reached because the check route stayed busy.",
        ),
        (
            CheckSummary(
                stopped="the check route failed",
                statvar_count=5,
                checked_count=4,
                tables_checked=("a",),
            ),
            "Checked 4 of 5 StatVars across 1 table; 1 not reached because the "
            "check route failed.",
        ),
    ],
    ids=[
        "no url",
        "clean",
        "a table not checked",
        "every StatVar refused",
        "findings and refusals",
        "stopped with findings",
        "stopped without findings",
    ],
)
def test_the_first_line_is_the_verdict(summary: CheckSummary, verdict: str) -> None:
    assert first_line(render_report(summary)) == verdict


@pytest.mark.parametrize(
    ("summary", "lines"),
    [
        (
            CheckSummary(not_run="the check could not complete", failure_detail="E"),
            ["What stopped it: `E`.", ADVISORY_LINE],
        ),
        (
            CheckSummary(statvar_count=1, checked_count=1, generations=("g1",)),
            [ADVISORY_LINE, SEVERITY_LINE, "graph generation `g1`."],
        ),
        (
            CheckSummary(statvar_count=1, checked_count=1, generations=(None,)),
            [SEVERITY_LINE, "did not name the graph generation"],
        ),
        (
            CheckSummary(statvar_count=1, checked_count=1, generations=("g1", "g2")),
            ["generations `g1` and `g2`, as the graph changed"],
        ),
    ],
    ids=["did not run", "one generation", "no generation", "two generations"],
)
def test_the_report_says_what_the_check_is_under_the_verdict(
    summary: CheckSummary, lines: list[str]
) -> None:
    intro = render_report(summary).split("\n\n")[1]

    for line in lines:
        assert line in intro


def test_text_from_the_route_cannot_mention_link_or_break_the_comment() -> None:
    hostile = "a`b|c @someone [x](https://e.test)\n" + "z" * 200
    summary = CheckSummary(
        statvar_count=2,
        checked_count=1,
        route_refused_count=1,
        tables_checked=("a",),
        tables_with_findings=1,
        error_count=1,
        failure_detail=hostile,
        stopped="the check route failed",
    )
    finding = Finding(
        table="a", statvar="V", field="f", value=hostile, code=hostile, severity="error"
    )
    refused = RefusedRow(table="a", statvar="W", by_route=True, detail=hostile)

    report = render_report(summary, findings=[finding], refused=[refused])

    assert "@" not in report
    assert "a'b\\|c" in report
    assert "z" * MAX_VALUE_CHARACTERS not in report
    for line in report.splitlines():
        if hostile[:3].replace("`", "'") in line:
            assert line.count("`") % 2 == 0


def test_findings_are_tabled_per_table_with_examples_and_explained() -> None:
    summary = CheckSummary(
        statvar_count=3,
        checked_count=1,
        route_refused_count=1,
        unsent_count=1,
        tables_checked=("oecd.statvars",),
        tables_with_findings=1,
        unchecked_tables=(("imf.statvars", "has no rows in this build"),),
        error_count=1,
        generations=("g1",),
    )
    refused = [
        RefusedRow(
            table="oecd.statvars", statvar="Variable2", by_route=True, detail="bad"
        ),
        RefusedRow(table="oecd.statvars", statvar="Variable3", by_route=False),
    ]

    report = render_report(summary, findings=[make_finding()], refused=refused)

    assert (
        "| `node_not_held` | error | 1 | `Variable1` "
        "`constraint_properties.crsPurpose` = `CRSPurposeCode99999` |"
    ) in report
    assert "The check route refused 1 StatVar even when sent alone" in report
    assert "- `Variable2`: `bad`" in report
    assert "1 StatVar had no measured property" in report
    assert "- `Variable3`\n" in report
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
        checked_count=len(findings),
        tables_checked=tuple(tables),
        tables_with_findings=len(tables),
        error_count=len(findings),
        generations=("g1",),
    )

    report = render_report(summary, findings=findings)

    assert len(report) <= MAX_REPORT_CHARACTERS
    assert first_line(report).startswith("Found 1,200 errors in 1,200 StatVars")
    assert "the counts are complete" in report
    assert "more tables with" in report
