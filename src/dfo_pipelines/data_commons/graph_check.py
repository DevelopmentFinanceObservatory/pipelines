"""Check an export's StatVar definitions against the Graph API's served schema.

The Graph API's `POST /api/graph/schema/check` judges candidate StatVar rows against
the graph the deployment serves: whether each node a row names exists, has the right
kind, and fits the ranges and domains of DFO's properties. The route checks the first
`rows_checked` rows of a request, at least one, and the caller resumes at that index.
It answers 503 while it builds its schema and 429 when a caller has spent its
allowance, each with `Retry-After`.

The check reads the served graph, so a node the bundle defines is absent from it
until the bundle is ingested. A StatVar the bundle defines, named as another row's
value (a denominator, say), would read as missing, so findings whose value is a dcid
in the StatVar model's `dcid` column are dropped.
"""

import os
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

# The deployment origin the check posts to, e.g. https://developmentfinance.org.
GRAPH_API_URL_VARIABLE = "DFO_GRAPH_API_URL"

CHECK_PATH = "/api/graph/schema/check"

# The most rows one request carries, the route's `CHECK_MAX_ROWS`.
CHECK_MAX_ROWS = 2_000

# The route gives each check a 30 s deadline; this leaves room for the hops before it.
REQUEST_TIMEOUT_SECONDS = 40.0

# The most time a check spends waiting out 503 and 429 answers, summed over its
# requests.
RETRY_WAIT_LIMIT_SECONDS = 120.0

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
    """The check could not finish: the Graph API is unset or unreachable, kept
    answering 503 or 429 past `RETRY_WAIT_LIMIT_SECONDS`, or answered something the
    check cannot read."""


@dataclass(frozen=True, kw_only=True)
class GraphFinding:
    """One fault the Graph API found in one StatVar.

    Attributes:
        statvar: The StatVar's dcid, without the `dcid:` prefix.
        field: The row member the finding is about, e.g. `stat_type` or
            `constraint_properties`.
        property: The constraint or observation property the finding is about;
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


def graph_api_url() -> str | None:
    """The origin `GRAPH_API_URL_VARIABLE` names, or None when it is unset or empty"""
    return os.environ.get(GRAPH_API_URL_VARIABLE) or None


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
    return max(0.0, (when - datetime.now(UTC)).total_seconds())


@dataclass
class _Session:
    client: httpx.Client
    sleep: Callable[[float], None]
    waited: float = 0.0

    def post(self, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        """The route's answer to `rows`, after waiting out 503 and 429 answers"""
        while True:
            try:
                response = self.client.post(CHECK_PATH, json={"rows": list(rows)})
            except httpx.HTTPError as error:
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
            if self.waited + wait > RETRY_WAIT_LIMIT_SECONDS:
                raise GraphCheckError(
                    f"the Graph API answered {response.status_code} past "
                    f"{RETRY_WAIT_LIMIT_SECONDS:.0f} s of waiting"
                )
            self.sleep(wait)
            self.waited += wait
        if response.status_code != 200:
            raise GraphCheckError(
                f"the Graph API answered {response.status_code}: {response.text[:500]}"
            )
        return response.json()


def check_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    client: httpx.Client,
    sleep: Callable[[float], None] = time.sleep,
) -> list[tuple[int, dict[str, Any]]]:
    """Every finding the route makes over `rows`, with the index of its row.

    Each request carries up to `CHECK_MAX_ROWS` rows from the first unchecked one,
    and the next starts after the rows its answer checked.

    Raises:
        GraphCheckError: If the Graph API cannot be reached, answers other than 200
            once the 503 and 429 answers are waited out, or answers checking no
            row, more rows than sent, or a finding past the rows checked.
    """
    session = _Session(client=client, sleep=sleep)
    findings: list[tuple[int, dict[str, Any]]] = []
    start = 0
    while start < len(rows):
        batch = rows[start : start + CHECK_MAX_ROWS]
        answer = session.post(batch)
        checked = answer["rows_checked"]
        if not 1 <= checked <= len(batch):
            raise GraphCheckError(
                f"the Graph API checked {checked} of {len(batch)} rows sent"
            )
        for finding in answer["findings"]:
            if not 0 <= finding["row"] < checked:
                raise GraphCheckError(
                    f"a finding names row {finding['row']} of {checked} checked"
                )
            findings.append((start + finding["row"], finding))
        start += checked
    return findings


def check_statvars(
    statvars: Iterable[Mapping[str, Any]],
    *,
    column_to_property: Mapping[str, str],
    client: httpx.Client,
    sleep: Callable[[float], None] = time.sleep,
) -> list[GraphFinding]:
    """The findings the Graph API makes over a StatVar model's rows, less those
    whose value is a StatVar the rows define.

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
    return [
        GraphFinding(
            statvar=rows[index]["dcid"],
            field=finding["field"],
            property=finding["property"],
            value=finding["value"],
            code=finding["code"],
            severity=finding["severity"],
        )
        for index, finding in check_rows(rows, client=client, sleep=sleep)
        # A finding on the row's own dcid judges its syntax, which ingestion
        # does not change.
        if finding["field"] == "dcid"
        or not (isinstance(finding["value"], str) and finding["value"] in defined)
    ]
