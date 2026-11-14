"""Render a graph check as Markdown for a pull request comment.

The first line is the verdict, followed by the commit checked, when it is known,
and a few lines on what the check is and what it was checked against. Below them,
each table with findings gets a short table of finding codes with a count and a
few examples, then the StatVars that were not checked, then each table that was
not checked gets a line, then the codes that appear are explained.

Every value that comes from the check route or from a table is shown as a code
span that cannot close early, break a table row, mention a user or link.
"""

import unicodedata
from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

# What each finding code the route returns means, for a reader who has not seen
# the route.
FINDING_CODES = {
    "node_not_held": (
        "The graph holds no node with this dcid. Check the spelling, or add the "
        "node to the graph."
    ),
    "node_untyped": (
        "The graph holds this node but gives it no type. Give the node a type."
    ),
    "node_provisional": (
        "The graph holds this node only as a placeholder (`ProvisionalNode`). "
        "Define the node in the graph."
    ),
    "value_missing": (
        "The StatVar names this constraint property but gives it no value. "
        "Fill in the value."
    ),
    "not_a_property": (
        "The StatVar uses this as a property, but the graph does not define it as "
        "one. Define it as a property, or use another one."
    ),
    "not_a_class": (
        "The StatVar uses this as a population type, but the graph does not define "
        "it as a class. Define it as a class, or use another type."
    ),
    "value_outside_range": (
        "The property does not allow this value. Use a class or code from the ones "
        "the property ranges over."
    ),
    "property_outside_domain": (
        "The graph does not declare this property for the population type or any "
        "of its parent classes. Use a property the type allows, or declare this "
        "one for it."
    ),
    "definition_differs": (
        "The graph already holds this StatVar with a different value here. The "
        "value shown is the graph's."
    ),
}

# The explanation of a finding code the report has no text for.
UNKNOWN_CODE = (
    "A finding this report does not describe. See the Graph API documentation."
)

ADVISORY_LINE = (
    "This check is advisory and does not block the merge. It compares the StatVar "
    "definitions this pull request builds with the knowledge graph."
)

SEVERITY_LINE = (
    "An error is a definition the graph would not accept. A warning is one to look at."
)

# The most examples shown for one finding code in one table.
MAX_EXAMPLES = 5

# The longest report rendered. GitHub caps a comment at 65,536 characters.
MAX_REPORT_CHARACTERS = 40_000

# The longest value shown in a code span.
MAX_VALUE_CHARACTERS = 80

# The characters of a commit hash shown.
SHORT_COMMIT_LENGTH = 7

# The note in a report too long to list any finding.
FINDINGS_LEFT_OUT_NOTE = (
    "The findings are not listed to keep this comment short. The counts are complete."
)


@dataclass(frozen=True, kw_only=True)
class Finding:
    """One finding, placed in the table and StatVar it is about.

    `severity` is `error` or `warning`, as the client checks. Every other member
    is shown as a code span.
    """

    table: str
    statvar: str
    field: str
    value: Any
    code: str
    severity: str


@dataclass(frozen=True, kw_only=True)
class RefusedRow:
    """A StatVar that was not checked, with who refused it and why.

    Attributes:
        table: The StatVar's table.
        statvar: The StatVar's dcid.
        by_route: True when the check route refused it. False when it was not
            sent, for want of a measured property, which the route requires.
        detail: The route's reason, or None when it was not sent.
    """

    table: str
    statvar: str
    by_route: bool
    detail: str | None = None


@dataclass(frozen=True, kw_only=True)
class CheckSummary:
    """What a graph check found, or why it did not run.

    Attributes:
        not_run: Why no StatVar was checked, or None when some were.
        stopped: Why the check stopped before every StatVar was sent, or None.
        failure_detail: The detail of the failure behind `not_run` or `stopped`.
        statvar_count: The StatVars in the tables read.
        checked_count: The StatVars the route answered.
        route_refused_count: The StatVars the route refused, each sent alone.
        unsent_count: The StatVars not sent, for want of a measured property.
        tables_checked: The tables whose StatVars were read.
        unchecked_tables: Each table that was not checked, with the reason.
        error_count: Findings of severity `error`.
        warning_count: Findings of severity `warning`.
        generations: The graph generations the answers named.
        commit: The pull request head commit the build was made for, or None when
            it is not known.
    """

    not_run: str | None = None
    stopped: str | None = None
    failure_detail: str | None = None
    statvar_count: int = 0
    checked_count: int = 0
    route_refused_count: int = 0
    unsent_count: int = 0
    tables_checked: tuple[str, ...] = ()
    unchecked_tables: tuple[tuple[str, str], ...] = ()
    error_count: int = 0
    warning_count: int = 0
    generations: tuple[str | None, ...] = ()
    commit: str | None = None

    @property
    def ran(self) -> bool:
        """Whether any StatVar reached the route and was answered or refused"""
        return self.not_run is None

    @property
    def unreached_count(self) -> int:
        """The StatVars the check did not reach because it stopped early"""
        return (
            self.statvar_count
            - self.checked_count
            - self.route_refused_count
            - self.unsent_count
        )


def render_report(
    summary: CheckSummary,
    *,
    findings: Sequence[Finding] = (),
    refused: Sequence[RefusedRow] = (),
) -> str:
    """The Markdown report of a graph check, at most `MAX_REPORT_CHARACTERS` long.

    When the full report is too long, examples are cut first, then the tables
    with the fewest findings are left out, and the report says which it did. A
    report too long with every table left out keeps its head and says the
    findings are not listed.

    Args:
        summary: The check's counts, or why it did not run.
        findings: Every finding.
        refused: Every StatVar that was not checked, with who refused it.
    """
    head = [_verdict(summary), "", *_commit(summary), *_intro(summary)]
    if not summary.ran:
        return "\n".join(head) + "\n"

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
        report = _join(head, notes, sections, footer)
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
        report = _join(head, notes, sections[:shown], footer)
        if len(report) <= MAX_REPORT_CHARACTERS:
            return report
    return _join(head, [FINDINGS_LEFT_OUT_NOTE], [], [])


def failure_report(detail: str, *, commit: str | None = None) -> str:
    """The report of a check that failed before it could report what it found"""
    return render_report(
        CheckSummary(
            not_run="the check could not complete",
            failure_detail=detail,
            commit=commit,
        )
    )


def _verdict(summary: CheckSummary) -> str:
    """The report's first line"""
    if summary.not_run is not None:
        return f"The graph check did not run: {summary.not_run}."

    total = summary.statvar_count
    checked = summary.checked_count
    statvars = (
        _count(checked, "StatVar")
        if checked == total
        else f"{checked:,} of {_count(total, 'StatVar')}"
    )
    tables = _count(len(summary.tables_checked), "table")
    problems = " and ".join(
        _count(count, noun)
        for count, noun in (
            (summary.error_count, "error"),
            (summary.warning_count, "warning"),
        )
        if count
    )
    left = [
        f"{_were(count)} {what}."
        for count, what in (
            (summary.route_refused_count, "refused by the check route"),
            (summary.unsent_count, "not sent"),
            (summary.unreached_count, f"not reached because {summary.stopped}"),
        )
        if count
    ]
    if summary.unchecked_tables:
        left.append(f"{_were(len(summary.unchecked_tables), 'table')} not checked.")
    if problems:
        head = f"Found {problems} in {statvars} checked across {tables}"
    elif left:
        head = f"Checked {statvars} across {tables}"
    else:
        head = f"No problems found in {statvars} across {tables}"
    return " ".join([f"{head}.", *left])


def _commit(summary: CheckSummary) -> list[str]:
    """The line naming the commit checked, when it is known"""
    if not summary.commit:
        return []
    return [
        f"Checked at commit {_code_span(summary.commit[:SHORT_COMMIT_LENGTH])}.",
        "",
    ]


def _intro(summary: CheckSummary) -> list[str]:
    """The lines under the verdict: what failed, what the check is, which graph"""
    lines = []
    if summary.failure_detail:
        lines.append(f"What stopped it: {_code_span(summary.failure_detail)}.")
    lines.append(ADVISORY_LINE)
    if summary.ran:
        lines.append(SEVERITY_LINE)
        lines.append(_generations(summary.generations))
    return [" ".join(lines), ""]


def _generations(generations: Sequence[str | None]) -> str:
    """The graph generations the answers were checked against, as a sentence"""
    named = [_code_span(generation) for generation in generations if generation]
    if not named:
        return "The check service did not name the graph generation it checked against."
    if len(named) == 1:
        return f"It checked against graph generation {named[0]}."
    return (
        f"It checked against graph generations {', '.join(named[:-1])} and "
        f"{named[-1]}, as the graph changed during the check."
    )


def _table_section(
    table: str,
    findings: list[Finding],
    refused: list[RefusedRow],
    examples: int,
) -> list[str]:
    """One table's findings and the StatVars it could not check"""
    lines = [f"### {_code_span(table)}", ""]
    if findings:
        groups = Counter((finding.code, finding.severity) for finding in findings)
        header = "| Finding | Severity | Count |" + (" Examples |" if examples else "")
        lines += [header, "| --- | --- | ---: |" + (" --- |" if examples else "")]
        for (code, severity), count in sorted(
            groups.items(), key=lambda item: (item[0][1] != "error", -item[1], item[0])
        ):
            row = f"| {_cell(code)} | {severity} | {count:,} |"
            if examples:
                shown = [
                    _example(finding)
                    for finding in findings
                    if (finding.code, finding.severity) == (code, severity)
                ][:examples]
                row += f" {'<br>'.join(shown)} |"
            lines.append(row)
        lines.append("")
    by_route = [row for row in refused if row.by_route]
    if by_route:
        lines.append(
            f"The check route refused {_count(len(by_route), 'StatVar')} even when "
            "sent alone, so they were not checked:"
        )
        lines += _listed(
            [
                f"{_code_span(row.statvar)}: {_code_span(row.detail or '')}"
                for row in by_route
            ],
            examples,
        )
    unsent = [row for row in refused if not row.by_route]
    if unsent:
        lines.append(
            f"{_count(len(unsent), 'StatVar')} had no measured property, which the "
            "check route requires, so they were not sent:"
        )
        lines += _listed([_code_span(row.statvar) for row in unsent], examples)
    return lines


def _listed(items: list[str], examples: int) -> list[str]:
    """Items as a Markdown list, cut to `examples` (at least one) and a count"""
    shown = max(examples, 1)
    lines = [f"- {item}" for item in items[:shown]]
    if len(items) > shown:
        lines.append(f"- and {len(items) - shown:,} more")
    return [*lines, ""]


def _example(finding: Finding) -> str:
    """One finding as a table cell: StatVar, field and value"""
    return (
        f"{_cell(finding.statvar)} {_cell(finding.field)} = "
        f"{_cell(_value_text(finding.value))}"
    )


def _value_text(value: Any) -> str:
    """A finding's value as text"""
    if value is None:
        return "null"
    if isinstance(value, list):
        return ", ".join(str(item) for item in value) or "(none)"
    return str(value)


def _code_span(text: str) -> str:
    """Text as a Markdown code span that is safe to show from any source.

    Control and format characters (Unicode categories `Cc` and `Cf`, such as NUL
    and the characters that reverse text) are dropped, and whitespace collapses
    to single spaces. The text is cut to `MAX_VALUE_CHARACTERS`, a backtick
    becomes a quote so the span cannot close early, and `@` becomes a full-width
    at sign so no user is mentioned. Inside a code span GitHub renders no link.
    """
    text = " ".join(str(text).split())
    text = "".join(c for c in text if unicodedata.category(c) not in {"Cc", "Cf"})
    if len(text) > MAX_VALUE_CHARACTERS:
        text = text[: MAX_VALUE_CHARACTERS - 1] + "…"
    text = text.replace("`", "'").replace("@", "\N{FULLWIDTH COMMERCIAL AT}")
    return f"`{text}`" if text else "` `"


def _cell(text: str) -> str:
    """Text as a code span in a table cell, with `|` escaped so the row holds"""
    return _code_span(text).replace("|", "\\|")


def _unchecked_lines(summary: CheckSummary) -> list[str]:
    """One line per table that was not checked"""
    if not summary.unchecked_tables:
        return []
    return [
        f"- {_code_span(table)} {reason}, so it was not checked."
        for table, reason in summary.unchecked_tables
    ] + [""]


def _code_lines(findings: Sequence[Finding]) -> list[str]:
    """The meaning of each finding code the report shows"""
    codes = sorted({finding.code for finding in findings})
    if not codes:
        return []
    return ["#### What the findings mean", ""] + [
        f"- {_code_span(code)}: {FINDING_CODES.get(code, UNKNOWN_CODE)}"
        for code in codes
    ]


def _examples_cut_note(examples: int) -> str:
    """Says the examples were cut and the counts were not"""
    if examples == 0:
        return (
            "Examples are left out to keep this comment short. The counts are complete."
        )

    return (
        f"Examples are cut to {examples} per finding to keep this comment short. "
        "The counts are complete."
    )


def _join(
    head: list[str], notes: list[str], sections: list[list[str]], footer: list[str]
) -> str:
    """The report's parts as one Markdown document"""
    lines = list(head)
    if notes:
        lines += [*notes, ""]
    for section in sections:
        lines += section
    lines += footer
    return "\n".join(lines).rstrip() + "\n"


def _were(n: int, noun: str | None = None) -> str:
    """`n`, then `noun` if given, then `was` when `n` is one and `were` otherwise"""
    subject = f"{n:,}" if noun is None else _count(n, noun)
    return f"{subject} {'was' if n == 1 else 'were'}"


def _count(n: int, noun: str) -> str:
    """`n` and `noun`, plural unless `n` is one"""
    return f"{n:,} {noun}" if n == 1 else f"{n:,} {noun}s"
