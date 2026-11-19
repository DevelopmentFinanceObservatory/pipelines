import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from dfo_pipelines.data_commons.graph_check import (
    CHECK_MAX_ROWS,
    CHECK_PATH,
    RETRY_WAIT_LIMIT_SECONDS,
    GraphCheckError,
    GraphFinding,
    check_statvars,
)

BASE_URL = "https://graph.example"
COLUMN_TO_PROPERTY = {"name": "name", "aid_type": "aidType"}

type Handler = Callable[[list[dict[str, Any]]], httpx.Response]


def statvar(dcid: str, **columns: Any) -> dict[str, Any]:
    """A StatVar model row as dcp-tools sheets write it"""
    return {
        "dcid": f"dcid:{dcid}",
        "name": dcid,
        "population_type": "dcid:Flow",
        "measured_property": "dcid:amount",
        "stat_type": "dcid:measuredValue",
        "measurement_qualifier": None,
        "measurement_denominator": None,
        "constraint_properties": "aidType",
        "aid_type": "dcid:AidType1",
        "observation_properties": "financingProvider, financingRecipient",
        **columns,
    }


def finding(row: int, **fields: Any) -> dict[str, Any]:
    """A finding as the route encodes it"""
    return {
        "row": row,
        "field": "constraint_properties",
        "property": "aidType",
        "subject": "value",
        "value": "AidType1",
        "served": None,
        "code": "node_not_held",
        "severity": "error",
        **fields,
    }


def answer(rows_checked: int, *findings: dict[str, Any]) -> httpx.Response:
    return httpx.Response(
        200,
        json={"generation": "g1", "rows_checked": rows_checked, "findings": findings},
    )


class Route:
    """A stub of the check route answering each request through `handler`, and
    recording the rows each request sent"""

    def __init__(self, handler: Handler) -> None:
        self.handler = handler
        self.requests: list[list[dict[str, Any]]] = []
        self.waits: list[float] = []

    def _answer(self, request: httpx.Request) -> httpx.Response:
        assert request.url.path == CHECK_PATH
        rows = json.loads(request.content)["rows"]
        self.requests.append(rows)
        return self.handler(rows)

    def check(self, statvars: list[dict[str, Any]]) -> list[GraphFinding]:
        client = httpx.Client(
            base_url=BASE_URL, transport=httpx.MockTransport(self._answer)
        )
        return check_statvars(
            statvars,
            column_to_property=COLUMN_TO_PROPERTY,
            client=client,
            sleep=self.waits.append,
        )


def test_check_statvars_sends_each_row_in_the_routes_shape() -> None:
    """Test a row's `dcid:` prefixes are dropped, its constraint values are read
    from the columns the spec maps, and its observation properties are listed"""
    route = Route(lambda rows: answer(len(rows)))

    route.check([statvar("Variable1", measurement_denominator="dcid:Count_Person")])

    assert route.requests == [
        [
            {
                "dcid": "Variable1",
                "population_type": "Flow",
                "measured_property": "amount",
                "stat_type": "measuredValue",
                "measurement_qualifier": None,
                "measurement_denominator": "Count_Person",
                "constraint_properties": {"aidType": "AidType1"},
                "observation_properties": ["financingProvider", "financingRecipient"],
            }
        ]
    ]


def test_check_statvars_reads_an_unmapped_constraint_from_its_own_column() -> None:
    """Test a constraint property the spec maps no column onto takes the column
    named for it"""
    route = Route(lambda rows: answer(len(rows)))

    route.check(
        [statvar("Variable1", constraint_properties="sector", sector="dcid:S1")]
    )

    assert route.requests[0][0]["constraint_properties"] == {"sector": "S1"}


def test_check_statvars_resumes_after_the_rows_an_answer_checked() -> None:
    """Test a partial answer is followed by a request from the first unchecked
    row, and each finding is placed on the row it names in its own request"""
    answers = iter(
        [
            answer(2, finding(1)),
            answer(1, finding(0, value="AidType3")),
        ]
    )
    route = Route(lambda rows: next(answers))
    statvars = [statvar(f"Variable{index}") for index in range(1, 4)]

    findings = route.check(statvars)

    assert [[row["dcid"] for row in rows] for rows in route.requests] == [
        ["Variable1", "Variable2", "Variable3"],
        ["Variable3"],
    ]
    assert [(f.statvar, f.value) for f in findings] == [
        ("Variable2", "AidType1"),
        ("Variable3", "AidType3"),
    ]


def test_check_statvars_sends_at_most_the_routes_row_limit() -> None:
    """Test a model larger than one request's limit goes in several requests"""
    route = Route(lambda rows: answer(len(rows)))

    route.check([statvar(f"Variable{index}") for index in range(CHECK_MAX_ROWS + 1)])

    assert [len(rows) for rows in route.requests] == [CHECK_MAX_ROWS, 1]


def test_check_statvars_waits_out_a_503_with_retry_after() -> None:
    """Test a 503 carrying `Retry-After` is waited out and the rows sent again"""
    answers = iter(
        [httpx.Response(503, headers={"Retry-After": "7"}), answer(1, finding(0))]
    )
    route = Route(lambda rows: next(answers))

    findings = route.check([statvar("Variable1")])

    assert route.waits == [7.0]
    assert len(route.requests) == 2
    assert [f.code for f in findings] == ["node_not_held"]


def test_check_statvars_gives_up_past_the_wait_limit() -> None:
    """Test a route that keeps asking for waits fails the check once the next wait
    would pass the limit"""
    wait = RETRY_WAIT_LIMIT_SECONDS / 2
    route = Route(
        lambda rows: httpx.Response(429, headers={"Retry-After": f"{wait:.0f}"})
    )

    with pytest.raises(GraphCheckError):
        route.check([statvar("Variable1")])

    assert route.waits == [wait, wait]


@pytest.mark.parametrize(
    "response",
    [
        pytest.param(answer(0), id="no row checked"),
        pytest.param(answer(2), id="more rows than sent"),
        pytest.param(answer(1, finding(1)), id="finding past the rows checked"),
        pytest.param(httpx.Response(503), id="503 without Retry-After"),
        pytest.param(httpx.Response(400, json={}), id="refusal"),
    ],
)
def test_check_statvars_fails_on_an_answer_it_cannot_place(
    response: httpx.Response,
) -> None:
    """Test an answer that would leave rows unchecked or misplace a finding fails
    the check"""
    route = Route(lambda rows: response)

    with pytest.raises(GraphCheckError):
        route.check([statvar("Variable1")])


def test_check_statvars_fails_when_the_graph_api_is_unreachable() -> None:
    """Test a transport error fails the check"""

    def refuse(rows: list[dict[str, Any]]) -> httpx.Response:
        raise httpx.ConnectError("refused")

    with pytest.raises(GraphCheckError):
        Route(refuse).check([statvar("Variable1")])


def test_check_statvars_drops_findings_on_statvars_the_rows_define() -> None:
    """Test a finding whose value is a StatVar the rows define is dropped, since
    the graph lacks it until ingestion, while one on a row's own dcid is kept"""
    route = Route(
        lambda rows: answer(
            2,
            finding(0, field="measurement_denominator", value="Variable2"),
            finding(1, field="dcid", property=None, value="Variable2"),
            finding(1, value="AidType9"),
        )
    )

    findings = route.check(
        [
            statvar("Variable1", measurement_denominator="dcid:Variable2"),
            statvar("Variable2"),
        ]
    )

    assert [(f.statvar, f.field, f.value) for f in findings] == [
        ("Variable2", "dcid", "Variable2"),
        ("Variable2", "constraint_properties", "AidType9"),
    ]
