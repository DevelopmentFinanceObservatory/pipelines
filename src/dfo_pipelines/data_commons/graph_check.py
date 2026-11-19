"""Check an export's StatVar definitions against the Graph API's served schema.

The Graph API's `POST /api/graph/schema/check` judges candidate StatVar rows against
the graph the deployment serves: whether each node a row names exists, has the right
kind, and fits the ranges and domains of DFO's properties. The route checks the first
`rows_checked` rows of a request, at least one, and the caller resumes at that index.
It refuses a row that does not fit its request model with a 400 whose `pointer` is
`/rows/<i>` or a member under it; the check reports that row and carries on without
it. It answers 503 while it builds its schema and 429 when a caller has spent its
allowance, each with `Retry-After`.

The check reads the served graph, so a node the bundle defines is absent from it
until the bundle is ingested. A StatVar the bundle defines, named as another row's
value (a denominator, say), would read as missing, so `node_not_held` and
`node_untyped` findings whose value is a dcid in the StatVar model's `dcid` column
are dropped.
"""

import os
import re
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Any, cast

import httpx

if TYPE_CHECKING:
    from dfo_pipelines.data_commons.export import ExportSpec

# The deployment origin the check posts to, e.g. https://developmentfinance.org.
GRAPH_API_URL_VARIABLE = "DFO_GRAPH_API_URL"

CHECK_PATH = "/api/graph/schema/check"

# The most rows one request carries, the route's `CHECK_MAX_ROWS`.
CHECK_MAX_ROWS = 2_000

# The route gives each check a 30 s deadline. This leaves room for the hops before it.
REQUEST_TIMEOUT_SECONDS = 40.0

# The most time a check spends waiting out 503 and 429 answers, summed over its
# requests.
RETRY_WAIT_LIMIT_SECONDS = 120.0

# The least time one wait counts for, so that a `Retry-After` of 0 or a past date
# still spends the wait limit.
RETRY_WAIT_FLOOR_SECONDS = 1.0

# The finding codes a node the bundle defines draws before it is ingested.
ABSENT_NODE_CODES = frozenset({"node_not_held", "node_untyped"})

# The members of a finding the check reads.
FINDING_MEMBERS = ("row", "field", "property", "value", "code", "severity")

# A refusal's pointer naming one row of the request: `/rows/<i>` or a member under it.
ROW_POINTER = re.compile(r"^/rows/(\d+)(?:/|$)")

# The StatVar model columns the route reads by name. The constraint values come
# from the columns `constraint_properties` names, through the spec's mapping.
SHAPE_COLUMNS = (
    "population_type",
    "measured_property",
    "stat_type",
    "measurement_qualifier",
    "measurement_denominator",
)

DCID_PREFIX = "dcid:"


class GraphCheckError(Exception):
    """The check could not finish: the Graph API's URL is invalid, the Graph API is
    unreachable, kept answering 503 or 429 past `RETRY_WAIT_LIMIT_SECONDS`, refused
    the request as a whole, or answered something the check cannot read."""


@dataclass(frozen=True, kw_only=True)
class GraphFinding:
    """One fault the Graph API found in one StatVar.

    Attributes:
        statvar: The StatVar's dcid, without the `dcid:` prefix.
        field: The row member the finding is about, e.g. `stat_type` or
            `constraint_properties`.
        property: The constraint or observation property the finding is about, or
            None for the other members.
        value: What the row sent in the judged cell.
        code: The route's finding code, e.g. `node_not_held`.
        severity: `error` or `warning`.
    """

    statvar: str | None
    field: str
    property: str | None
    value: Any
    code: str
    severity: str

    def __str__(self) -> str:
        cell = self.field if self.property is None else f"{self.field}.{self.property}"
        return (
            f"{self.statvar}: {self.code} ({self.severity}) in {cell}: {self.value!r}"
        )


@dataclass(frozen=True, kw_only=True)
class RefusedRow:
    """One StatVar the Graph API refused to check, because its row does not fit the
    route's request model.

    Attributes:
        statvar: The StatVar's dcid, without the `dcid:` prefix.
        pointer: Where in the request the refusal points, e.g.
            `/rows/0/constraint_properties`.
        detail: What the refusal says is wrong.
    """

    statvar: str | None
    pointer: str
    detail: str

    def __str__(self) -> str:
        return f"{self.statvar}: refused at {self.pointer}: {self.detail}"


@dataclass(frozen=True, kw_only=True)
class GraphCheck:
    """What the Graph API made of a StatVar model's rows.

    Attributes:
        findings: The faults found in the rows it checked.
        refused: The rows it refused to check.
    """

    findings: list[GraphFinding]
    refused: list[RefusedRow]

    @property
    def count(self) -> int:
        """Count of findings and refused rows"""
        return len(self.findings) + len(self.refused)

    @property
    def has_errors(self) -> bool:
        """Whether a finding is an error or a row was refused"""
        return bool(self.refused) or any(
            finding.severity == "error" for finding in self.findings
        )


def graph_api_url() -> str | None:
    """The origin `GRAPH_API_URL_VARIABLE` names, stripped of surrounding
    whitespace, or None when it is unset or blank"""
    return os.environ.get(GRAPH_API_URL_VARIABLE, "").strip() or None


def open_client(base_url: str) -> httpx.Client:
    """A client posting to the Graph API at `base_url`"""
    return httpx.Client(base_url=base_url, timeout=REQUEST_TIMEOUT_SECONDS)


def _reference(text: str) -> str:
    """A node reference with its leading `dcid:` dropped"""
    return text.strip().removeprefix(DCID_PREFIX).strip()


def _node(value: Any) -> str | int | float | bool | None:
    """A cell as the route reads it: a string's leading `dcid:` dropped, and a value
    JSON has no type for, such as a DuckDB decimal, sent as its text"""
    if isinstance(value, str):
        return _reference(value)
    if value is None or isinstance(value, int | float | bool):
        return value
    return str(value)


def _names(cell: str | None) -> list[str]:
    """The non-empty members of a comma-separated list, each a node reference"""
    if not cell:
        return []
    return [name for part in cell.split(",") if (name := _reference(part))]


def check_row(
    row: Mapping[str, Any], *, column_to_property: Mapping[str, str]
) -> dict[str, Any]:
    """One StatVar model row in the route's `SchemaCheckRow` shape.

    Each property `constraint_properties` names takes its value from the column
    `column_to_property` maps onto it, or else from the column named for it.
    """
    property_to_column = {prop: column for column, prop in column_to_property.items()}
    constraints = {
        prop: _node(row.get(property_to_column.get(prop, prop)))
        for prop in _names(row.get("constraint_properties"))
    }
    return {
        "dcid": _node(row.get("dcid")),
        **{column: _node(row.get(column)) for column in SHAPE_COLUMNS},
        "constraint_properties": constraints,
        "observation_properties": _names(row.get("observation_properties")),
    }


def _retry_after_seconds(response: httpx.Response) -> float | None:
    """Seconds `Retry-After` asks for, as delta seconds or an HTTP date, or None
    when the header is absent or unreadable"""
    header = response.headers.get("retry-after")
    if header is None:
        return None
    if header.strip().isdigit():
        return float(header)
    try:
        when = parsedate_to_datetime(header)
    except (TypeError, ValueError):
        return None
    # An HTTP date is in GMT, and a `-0000` zone parses as a naive datetime.
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - datetime.now(UTC)).total_seconds())


@dataclass(frozen=True)
class _Refusal:
    """A 400 whose pointer names row `row` of the request"""

    row: int
    pointer: str
    detail: str


def _refusal(response: httpx.Response) -> _Refusal | None:
    """The row a 400 refuses, or None when its pointer names no one row"""
    try:
        body = response.json()
    except ValueError:
        return None
    if not isinstance(body, dict) or not isinstance(body.get("pointer"), str):
        return None
    named = ROW_POINTER.match(body["pointer"])
    if named is None:
        return None
    detail = body.get("detail") or body.get("title") or "the Graph API refused the row"
    return _Refusal(row=int(named[1]), pointer=body["pointer"], detail=str(detail))


@dataclass
class _Session:
    client: httpx.Client
    sleep: Callable[[float], None]
    waited: float = 0.0

    def post(self, rows: Sequence[Mapping[str, Any]]) -> object:
        """The route's answer to `rows` after waiting out 503 and 429 answers, which
        is the parsed body of a 200 or the refusal of a 400 naming one row"""
        while True:
            try:
                response = self.client.post(CHECK_PATH, json={"rows": list(rows)})
            except (httpx.HTTPError, httpx.InvalidURL) as error:
                raise GraphCheckError(
                    f"the Graph API is unreachable: {error!r}"
                ) from error
            wait = (
                _retry_after_seconds(response)
                if response.status_code in (429, 503)
                else None
            )
            if wait is None:
                break
            wait = max(wait, RETRY_WAIT_FLOOR_SECONDS)
            if self.waited + wait > RETRY_WAIT_LIMIT_SECONDS:
                raise GraphCheckError(
                    f"the Graph API answered {response.status_code} past "
                    f"{RETRY_WAIT_LIMIT_SECONDS:.0f} s of waiting"
                )
            self.sleep(wait)
            self.waited += wait
        if response.status_code == 400 and (refusal := _refusal(response)):
            return refusal
        if response.status_code != 200:
            raise GraphCheckError(
                f"the Graph API answered {response.status_code}: {response.text[:500]}"
            )
        try:
            return response.json()
        except ValueError as error:
            raise GraphCheckError(
                f"the Graph API answered 200 without JSON: {response.text[:500]}"
            ) from error


def _read_answer(body: object, *, sent: int) -> tuple[int, list[dict[str, Any]]]:
    """The rows checked and the findings of a 200 answering `sent` rows.

    Raises:
        GraphCheckError: If the answer lacks a member the check reads, checks no
            row or more rows than sent, or has a finding past the rows checked.
    """
    if not isinstance(body, dict):
        raise GraphCheckError(f"the Graph API answered {type(body).__name__}")
    checked = body.get("rows_checked")
    answered = body.get("findings")
    if (
        not isinstance(checked, int)
        or isinstance(checked, bool)
        or not isinstance(answered, list)
    ):
        raise GraphCheckError(
            "the Graph API answered without an integer rows_checked and a list of"
            " findings"
        )
    if not 1 <= checked <= sent:
        raise GraphCheckError(f"the Graph API checked {checked} of {sent} rows sent")
    findings: list[dict[str, Any]] = []
    for finding in answered:
        if not isinstance(finding, dict) or not all(
            member in finding for member in FINDING_MEMBERS
        ):
            raise GraphCheckError(
                f"a finding lacks one of {', '.join(FINDING_MEMBERS)}: {finding!r}"
            )
        # Parsed JSON objects have string keys.
        finding = cast(dict[str, Any], finding)
        row = finding["row"]
        if not isinstance(row, int) or isinstance(row, bool) or not 0 <= row < checked:
            raise GraphCheckError(f"a finding names row {row!r} of {checked} checked")
        findings.append(finding)
    return checked, findings


def check_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    client: httpx.Client,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[list[tuple[int, dict[str, Any]]], list[tuple[int, _Refusal]]]:
    """Every finding the route makes over `rows`, and every row it refuses, each
    with the index of its row.

    Each request carries up to `CHECK_MAX_ROWS` of the rows neither checked nor
    refused, in order, and the next starts after the rows its answer checked. A 400
    naming one row of a request drops that row from the next.

    Raises:
        GraphCheckError: If the Graph API cannot be reached, answers other than 200
            or a 400 naming one row once the 503 and 429 answers are waited out, or
            answers something `_read_answer` cannot read.
    """
    session = _Session(client=client, sleep=sleep)
    findings: list[tuple[int, dict[str, Any]]] = []
    refused: list[tuple[int, _Refusal]] = []
    # The indices of the rows not refused. Those before `start` are checked.
    pending = list(range(len(rows)))
    start = 0
    while start < len(pending):
        sent = pending[start : start + CHECK_MAX_ROWS]
        answer = session.post([rows[index] for index in sent])
        if isinstance(answer, _Refusal):
            if answer.row >= len(sent):
                raise GraphCheckError(
                    f"the Graph API refused row {answer.row} of {len(sent)} sent"
                )
            refused.append((sent[answer.row], answer))
            del pending[start + answer.row]
            continue
        checked, answer_findings = _read_answer(answer, sent=len(sent))
        findings.extend((sent[finding["row"]], finding) for finding in answer_findings)
        start += checked
    return findings, refused


def check_statvars(
    statvars: Iterable[Mapping[str, Any]],
    *,
    column_to_property: Mapping[str, str],
    client: httpx.Client,
    sleep: Callable[[float], None] = time.sleep,
) -> GraphCheck:
    """What the Graph API makes of a StatVar model's rows, less the absent-node
    findings on StatVars the rows define.

    Args:
        statvars: The StatVar model's rows, keyed on column name.
        column_to_property: The export spec's `statvar_column_to_property`.
        client: Client whose base URL is the Graph API's origin.
        sleep: Called with the seconds to wait before retrying a 503 or 429.

    Raises:
        GraphCheckError: As `check_rows` does.
    """
    rows = [check_row(row, column_to_property=column_to_property) for row in statvars]
    defined = {row["dcid"] for row in rows if row["dcid"]}
    findings, refused = check_rows(rows, client=client, sleep=sleep)
    return GraphCheck(
        findings=[
            GraphFinding(
                statvar=rows[index]["dcid"],
                field=finding["field"],
                property=finding["property"],
                value=finding["value"],
                code=finding["code"],
                severity=finding["severity"],
            )
            for index, finding in findings
            if not (
                finding["code"] in ABSENT_NODE_CODES
                and isinstance(finding["value"], str)
                and finding["value"] in defined
            )
        ],
        refused=[
            RefusedRow(
                statvar=rows[index]["dcid"],
                pointer=refusal.pointer,
                detail=refusal.detail,
            )
            for index, refusal in refused
        ],
    )


def check_spec(
    spec: "ExportSpec",
    rows: Iterable[Mapping[str, Any]],
    *,
    base_url: str,
    sleep: Callable[[float], None] = time.sleep,
) -> GraphCheck:
    """What the Graph API at `base_url` makes of the rows of `spec`'s StatVar model.

    Args:
        spec: The export whose StatVar columns the rows carry.
        rows: The rows of `spec.statvar_model`, keyed on column name.
        base_url: The Graph API's origin, as `graph_api_url` gives it.
        sleep: Called with the seconds to wait before retrying a 503 or 429.

    Raises:
        GraphCheckError: If `base_url` is not a URL, or as `check_rows` does.
    """
    try:
        client = open_client(base_url)
    except httpx.InvalidURL as error:
        raise GraphCheckError(
            f"{GRAPH_API_URL_VARIABLE} is not a URL: {base_url!r}"
        ) from error
    with client:
        return check_statvars(
            rows,
            column_to_property=spec.statvar_column_to_property,
            client=client,
            sleep=sleep,
        )


def finding_groups(findings: Iterable[GraphFinding], *, examples: int = 3) -> list[str]:
    """One line per distinct code, field, property and value among `findings`,
    with its count and up to `examples` of the StatVars it names, most common
    first"""
    statvars: dict[tuple[str, str, str | None, str], list[str | None]] = {}
    for finding in findings:
        key = (finding.code, finding.field, finding.property, repr(finding.value))
        statvars.setdefault(key, []).append(finding.statvar)
    lines = []
    for (code, field, prop, value), names in sorted(
        statvars.items(), key=lambda item: -len(item[1])
    ):
        cell = field if prop is None else f"{field}.{prop}"
        named = ", ".join(str(name) for name in names[:examples])
        lines.append(
            f"{code} in {cell}: {value} on {plural(len(names), 'StatVar')}, e.g. {named}"
        )
    return lines


def plural(count: int, noun: str) -> str:
    """`count` and `noun`, with an `s` unless `count` is 1"""
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"
