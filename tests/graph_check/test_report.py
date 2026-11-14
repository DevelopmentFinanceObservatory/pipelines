import re

import pytest

from dfo_pipelines.graph_check.report import (
    ADVISORY_LINE,
    FINDING_CODES,
    FINDINGS_LEFT_OUT_NOTE,
    MAX_REPORT_CHARACTERS,
    MAX_VALUE_CHARACTERS,
    SEVERITY_LINE,
    CheckSummary,
    Finding,
    RefusedRow,
    render_report,
)
from tests.graph_check.tables import first_line, spans

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
    ("summary", "lines", "shown"),
    [
        (
            CheckSummary(not_run="the check could not complete", failure_detail="E"),
            [ADVISORY_LINE],
            ["E"],
        ),
        (
            CheckSummary(statvar_count=1, checked_count=1, generations=("g1",)),
            [ADVISORY_LINE, SEVERITY_LINE],
            ["g1"],
        ),
        (
            CheckSummary(statvar_count=1, checked_count=1, generations=(None,)),
            [ADVISORY_LINE, SEVERITY_LINE],
            [],
        ),
        (
            CheckSummary(statvar_count=1, checked_count=1, generations=("g1", "g2")),
            [ADVISORY_LINE, SEVERITY_LINE],
            ["g1", "g2"],
        ),
    ],
    ids=["did not run", "one generation", "no generation", "two generations"],
)
def test_the_report_says_what_the_check_is_under_the_verdict(
    summary: CheckSummary, lines: list[str], shown: list[str]
) -> None:
    intro = render_report(summary).split("\n\n")[1]

    for line in lines:
        assert line in intro
    assert spans(intro) == shown


def test_the_commit_checked_is_named_under_the_verdict() -> None:
    summary = CheckSummary(statvar_count=1, checked_count=1, commit="0123456789abcdef")

    paragraphs = render_report(summary).split("\n\n")

    assert spans(paragraphs[1]) == ["0123456"]
    assert ADVISORY_LINE in paragraphs[2]


def test_text_from_the_route_cannot_mention_link_or_break_the_comment() -> None:
    hostile = "a`b|c @someone \N{RIGHT-TO-LEFT OVERRIDE}\x00\x1b[x](https://e.test)\n"
    hostile += "z" * 200
    summary = CheckSummary(
        statvar_count=2,
        checked_count=1,
        route_refused_count=1,
        tables_checked=("a",),
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
    assert "\N{RIGHT-TO-LEFT OVERRIDE}" not in report
    assert "\x00" not in report
    assert "\x1b" not in report
    assert "z" * MAX_VALUE_CHARACTERS not in report
    lines = [line for line in report.splitlines() if "a'b" in line]
    cells = [line for line in lines if line.startswith("|")]
    assert len(cells) == 1
    assert len(lines) > len(cells)
    for line in lines:
        assert line.count("`") % 2 == 0
        assert ("a'b\\|c" in line) == (line in cells)
    # A row of four cells has five separators, whatever its cells hold.
    assert len(re.findall(r"(?<!\\)\|", cells[0])) == 5


def test_findings_are_tabled_per_table_with_examples_and_explained() -> None:
    summary = CheckSummary(
        statvar_count=3,
        checked_count=1,
        route_refused_count=1,
        unsent_count=1,
        tables_checked=("oecd.statvars",),
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

    lines = report.splitlines()
    assert (
        "| `node_not_held` | error | 1 | `Variable1` "
        "`constraint_properties.crsPurpose` = `CRSPurposeCode99999` |"
    ) in lines
    assert [spans(line) for line in lines if line.startswith("- ")] == [
        ["Variable2", "bad"],
        ["Variable3"],
        ["imf.statvars"],
        ["node_not_held"],
    ]
    assert f"- `node_not_held`: {FINDING_CODES['node_not_held']}" in lines


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
        error_count=len(findings),
        generations=("g1",),
    )

    report = render_report(summary, findings=findings)

    headings = [line for line in report.splitlines() if line.startswith("### ")]
    assert len(report) <= MAX_REPORT_CHARACTERS
    assert first_line(report) == (
        "Found 1,200 errors in 1,200 StatVars checked across 400 tables."
    )
    assert 0 < len(headings) < len(tables)


def findings_with_codes(codes: int, *, per_code: int = 5) -> list[Finding]:
    """`per_code` findings with long values for each of `codes` codes, in one table"""
    return [
        Finding(
            table="a",
            statvar=f"Variable{n}",
            field="f",
            value="v" * MAX_VALUE_CHARACTERS,
            code=f"code{code}",
            severity="error",
        )
        for code in range(codes)
        for n in range(per_code)
    ]


def one_table_summary(findings: list[Finding]) -> CheckSummary:
    """A clean run of one table with `findings`, all errors"""
    return CheckSummary(
        statvar_count=len(findings),
        checked_count=len(findings),
        tables_checked=("a",),
        error_count=len(findings),
    )


@pytest.mark.parametrize(
    ("codes", "examples"), [(60, 5), (100, 2), (200, 0)], ids=["5", "2", "0"]
)
def test_examples_are_cut_before_any_table_is_left_out(
    codes: int, examples: int
) -> None:
    findings = findings_with_codes(codes)

    report = render_report(one_table_summary(findings), findings=findings)

    rows = [line for line in report.splitlines() if line.startswith("| `")]
    assert len(report) <= MAX_REPORT_CHARACTERS
    assert len(rows) == codes
    assert {row.count("`Variable") for row in rows} == {examples}


def test_a_report_too_long_for_any_table_says_its_findings_are_not_listed() -> None:
    # Codes outside `FINDING_CODES` each take a line of their own to explain.
    findings = findings_with_codes(600, per_code=1)

    report = render_report(one_table_summary(findings), findings=findings)

    assert len(report) <= MAX_REPORT_CHARACTERS
    assert FINDINGS_LEFT_OUT_NOTE in report
    assert "`code0`" not in report
