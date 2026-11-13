"""Render a graph check as Markdown for a pull request comment.

The first line is the verdict. Below it, each table with findings gets a short
table of finding codes with a count and a few examples, then each table that was
not checked gets a line, then the codes that appear are explained.
"""

from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

# What each finding code the route returns means, for a reader who has not seen
# the route.
FINDING_CODES = {
    "node_not_held": "The graph holds no node with this dcid, or the value is not a dcid at all.",
    "node_untyped": "The graph holds this node but gives it no type.",
    "node_provisional": "The graph holds this node only as a placeholder (`ProvisionalNode`), not as a defined term.",
    "value_missing": "The StatVar names this constraint property but gives it no value.",
    "not_a_property": "This is used as a property, but the graph does not define it as one.",
    "not_a_class": "This is used as a population type, but the graph does not define it as a class.",
    "value_outside_range": "The value is not one the property allows: it is outside the classes or code lists the property ranges over.",
    "property_outside_domain": "The property is not declared for this population type or any of its parent classes.",
    "definition_differs": "The graph already holds this StatVar with a different value here; the value shown is the graph's.",
}

# The most examples shown for one finding code in one table.
MAX_EXAMPLES = 5

# The longest report rendered. GitHub caps a comment at 65,536 characters.
MAX_REPORT_CHARACTERS = 40_000

# The longest value shown in an example.
MAX_VALUE_CHARACTERS = 80


@dataclass(frozen=True, kw_only=True)
class Finding:
    """One finding, placed in the table and StatVar it is about."""

    table: str
    statvar: str
    field: str
    value: Any
    code: str
    severity: str


@dataclass(frozen=True, kw_only=True)
class RefusedRow:
    """A StatVar the route refused to check, with its reason."""

    table: str
    statvar: str
    detail: str


@dataclass(frozen=True, kw_only=True)
class CheckSummary:
    """What a graph check found, or why it did not run.

    Attributes:
        not_run: Why the check did not run, or None when it ran.
        statvar_count: StatVars sent to the route.
        tables_checked: The tables whose StatVars were sent.
        tables_with_findings: The tables with a finding or a refused StatVar.
        unchecked_tables: Each table that was not checked, with the reason.
        error_count: Findings of severity `error`.
        warning_count: Findings of severity `warning`.
        refused_count: StatVars the route refused to check.
        generations: The graph generations the answers named.
    """

    not_run: str | None = None
    statvar_count: int = 0
    tables_checked: tuple[str, ...] = ()
    tables_with_findings: int = 0
    unchecked_tables: tuple[tuple[str, str], ...] = ()
    error_count: int = 0
    warning_count: int = 0
    refused_count: int = 0
    generations: tuple[str | None, ...] = ()

    @property
    def ran(self) -> bool:
        """Whether any StatVar reached the route and was answered"""
        return self.not_run is None

    @property
    def clean(self) -> bool:
        """Whether every table was checked and nothing was found"""
        return (
            self.ran
            and not self.unchecked_tables
            and self.error_count == self.warning_count == self.refused_count == 0
        )


def render_report(
    summary: CheckSummary,
    *,
    findings: Sequence[Finding] = (),
    refused: Sequence[RefusedRow] = (),
) -> str:
    """The Markdown report of a graph check, at most `MAX_REPORT_CHARACTERS` long.

    When the full report is too long, examples are cut first, then the tables
    with the fewest findings are left out, and the report says which it did.

    Args:
        summary: The check's counts, or why it did not run.
        findings: Every finding.
        refused: Every StatVar the route refused to check.
    """
    verdict = _verdict(summary)
    if not summary.ran:
        return verdict + "\n"

    by_table: defaultdict[str, list[Finding]] = defaultdict(list)
    for finding in findings:
        by_table[finding.table].append(finding)
    refused_by_table: defaultdict[str, list[RefusedRow]] = defaultdict(list)
    for row in refused:
        refused_by_table[row.table].append(row)
    tables = sorted(
        by_table.keys() | refused_by_table.keys(),
        key=lambda table: (-len(by_table[table]) - len(refused_by_table[table]), table),
    )
    footer = _unchecked_lines(summary) + _code_lines(findings)

    for examples in (MAX_EXAMPLES, 2, 0):
        sections = [
            _table_section(table, by_table[table], refused_by_table[table], examples)
            for table in tables
        ]
        notes = [] if examples == MAX_EXAMPLES else [_examples_cut_note(examples)]
        report = _join(verdict, notes, sections, footer)
        if len(report) <= MAX_REPORT_CHARACTERS:
            return report

    for shown in range(len(sections) - 1, -1, -1):
        left_out = tables[shown:]
        count = sum(len(by_table[t]) + len(refused_by_table[t]) for t in left_out)
        notes = [
            _examples_cut_note(0),
            f"{_count(len(left_out), 'more table')} with {_count(count, 'finding')} "
            "are not listed here to keep this comment short.",
        ]
        report = _join(verdict, notes, sections[:shown], footer)
        if len(report) <= MAX_REPORT_CHARACTERS:
            return report
    return verdict + "\n"


def _verdict(summary: CheckSummary) -> str:
    """The report's first line"""
    if summary.not_run is not None:
        return f"The graph check did not run: {summary.not_run}."
    against = f"checked against {_generations(summary.generations)}"
    if summary.error_count == summary.warning_count == 0:
        head = (
            f"No problems found in {_count(summary.statvar_count, 'StatVar')} across "
            f"{_count(len(summary.tables_checked), 'table')}, {against}"
        )
    else:
        head = (
            f"Found {_count(summary.error_count, 'error')} and "
            f"{_count(summary.warning_count, 'warning')} across "
            f"{_count(summary.tables_with_findings, 'table')}, {against}"
        )
    caveats = []
    if summary.refused_count:
        caveats.append(
            f"{_count(summary.refused_count, 'StatVar')} could not be checked"
        )
    if unchecked := len(summary.unchecked_tables):
        were = "was" if unchecked == 1 else "were"
        caveats.append(f"{_count(unchecked, 'table')} {were} not checked")
    return "; ".join([head, *caveats]) + "."


def _generations(generations: Sequence[str | None]) -> str:
    """The graph generations, as the verdict names them"""
    named = [f"`{generation}`" for generation in generations if generation]
    if not named:
        return "the current graph (the service named no generation)"
    if len(named) == 1:
        return f"graph generation {named[0]}"
    return f"graph generations {', '.join(named[:-1])} and {named[-1]} (the graph changed during the check)"


def _table_section(
    table: str,
    findings: list[Finding],
    refused: list[RefusedRow],
    examples: int,
) -> list[str]:
    """One table's findings and refused StatVars"""
    lines = [f"### `{table}`", ""]
    if findings:
        groups = Counter((finding.code, finding.severity) for finding in findings)
        header = "| Finding | Severity | Count |" + (" Examples |" if examples else "")
        lines += [header, "| --- | --- | ---: |" + (" --- |" if examples else "")]
        for (code, severity), count in sorted(
            groups.items(), key=lambda item: (item[0][1] != "error", -item[1], item[0])
        ):
            row = f"| `{code}` | {severity} | {count:,} |"
            if examples:
                shown = [
                    _example(finding)
                    for finding in findings
                    if (finding.code, finding.severity) == (code, severity)
                ][:examples]
                row += f" {'<br>'.join(shown)} |"
            lines.append(row)
        lines.append("")
    if refused:
        lines.append(
            f"The check refused {_count(len(refused), 'StatVar')}, "
            "so they were not checked:"
        )
        lines += [
            f"- {_code_span(row.statvar)}: {_cell(row.detail)}"
            for row in refused[: max(examples, 1)]
        ]
        if len(refused) > max(examples, 1):
            lines.append(f"- and {len(refused) - max(examples, 1):,} more")
        lines.append("")
    return lines


def _example(finding: Finding) -> str:
    """One finding as a table cell: StatVar, field and value"""
    return (
        f"{_code_span(finding.statvar)} {_code_span(finding.field)} = "
        f"{_code_span(_value_text(finding.value))}"
    )


def _value_text(value: Any) -> str:
    """A finding's value as text"""
    if value is None:
        return "null"
    if isinstance(value, list):
        return ", ".join(str(item) for item in value) or "(none)"
    return str(value)


def _code_span(text: str) -> str:
    """Text as a Markdown code span that cannot break a table row"""
    if len(text) > MAX_VALUE_CHARACTERS:
        text = text[: MAX_VALUE_CHARACTERS - 1] + "…"
    return "`" + _cell(text.replace("`", "'")) + "`"


def _cell(text: str) -> str:
    """Text safe inside a Markdown table cell or list item"""
    return " ".join(text.split()).replace("|", "\\|")


def _unchecked_lines(summary: CheckSummary) -> list[str]:
    """One line per table that was not checked"""
    if not summary.unchecked_tables:
        return []
    return [
        f"- `{table}` {reason}, so it was not checked."
        for table, reason in summary.unchecked_tables
    ] + [""]


def _code_lines(findings: Sequence[Finding]) -> list[str]:
    """The meaning of each finding code the report shows"""
    codes = sorted({finding.code for finding in findings})
    if not codes:
        return []
    return ["#### What the findings mean", ""] + [
        f"- `{code}`: {FINDING_CODES.get(code, 'A finding this report does not describe; see the Graph API documentation.')}"
        for code in codes
    ]


def _examples_cut_note(examples: int) -> str:
    """Says the examples were cut and the counts were not"""
    if examples == 0:
        return (
            "Examples are left out to keep this comment short; the counts are complete."
        )
    return (
        f"Examples are cut to {examples} per finding to keep this comment short; "
        "the counts are complete."
    )


def _join(
    verdict: str, notes: list[str], sections: list[list[str]], footer: list[str]
) -> str:
    """The report's parts as one Markdown document"""
    lines = [verdict, ""]
    if notes:
        lines += [*notes, ""]
    for section in sections:
        lines += section
    lines += footer
    return "\n".join(lines).rstrip() + "\n"


def _count(n: int, noun: str) -> str:
    """`n` and `noun`, plural unless `n` is one"""
    return f"{n:,} {noun}" if n == 1 else f"{n:,} {noun}s"
