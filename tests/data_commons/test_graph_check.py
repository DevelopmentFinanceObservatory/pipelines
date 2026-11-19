import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from typing import Any

import httpx
import pytest

from dfo_pipelines.data_commons.graph_check import (
    CHECK_MAX_ROWS,
    CHECK_PATH,
    REFUSED_ROW_LIMIT,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_WAIT_LIMIT_SECONDS,
    GraphCheck,
    GraphCheckError,
    check_spec,
    check_statvars,
    open_client,
)
from dfo_pipelines.defs.export_specs import EXPORT_SPECS

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
    recording the rows each request sent, on a clock that each answer moves on by
    `answer_seconds` and each wait by the seconds waited"""

    def __init__(self, handler: Handler, *, answer_seconds: float = 0.0) -> None:
        self.handler = handler
        self.answer_seconds = answer_seconds
        self.requests: list[list[dict[str, Any]]] = []
        self.waits: list[float] = []
        self.timeouts: list[dict[str, float | None]] = []
        self.now = 0.0

    def _answer(self, request: httpx.Request) -> httpx.Response:
        assert request.url.path == CHECK_PATH
        rows = json.loads(request.content)["rows"]
        self.requests.append(rows)
        self.timeouts.append(request.extensions["timeout"])
        self.now += self.answer_seconds
        return self.handler(rows)

    def _sleep(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.now += seconds

    def check(
        self, statvars: list[dict[str, Any]], *, time_limit: float | None = None
    ) -> GraphCheck:
        client = httpx.Client(
            base_url=BASE_URL, transport=httpx.MockTransport(self._answer)
        )
        return check_statvars(
            statvars,
            column_to_property=COLUMN_TO_PROPERTY,
            client=client,
            sleep=self._sleep,
            time_limit=time_limit,
            clock=lambda: self.now,
        )


def test_check_statvars_sends_each_row_in_the_routes_shape() -> None:
    """Test a row's namespace prefixes are dropped, its constraint values are read
    from the columns the spec maps, and its observation properties are listed"""
    route = Route(lambda rows: answer(len(rows)))

    route.check(
        [
            statvar(
                "Variable1",
                measurement_denominator="dcs:Count_Person",
                aid_type="schema:AidType1",
            )
        ]
    )

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

    findings = route.check(statvars).findings

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

    findings = route.check([statvar("Variable1")]).findings

    assert route.waits == [7.0]
    assert len(route.requests) == 2
    assert [f.code for f in findings] == ["node_not_held"]


def test_check_statvars_gives_up_past_the_wait_limit() -> None:
    """Test a route that keeps asking for waits fails the check once the next wait
    would pass the limit, counting the waits of every request"""
    wait = f"{RETRY_WAIT_LIMIT_SECONDS * 0.75:.0f}"
    answers = iter(
        [
            httpx.Response(429, headers={"Retry-After": wait}),
            answer(1),
            httpx.Response(429, headers={"Retry-After": wait}),
        ]
    )
    route = Route(lambda rows: next(answers))

    with pytest.raises(GraphCheckError):
        route.check([statvar("Variable1"), statvar("Variable2")])

    assert route.waits == [float(wait)]
    assert len(route.requests) == 3


@pytest.mark.parametrize(
    ("statuses", "succeeds"),
    [
        pytest.param([502, 504], True, id="502 and 504 retried"),
        pytest.param([502, 502, 502, 502], False, id="past the gateway retries"),
    ],
)
def test_check_statvars_retries_a_gateway_error_a_few_times(
    statuses: list[int], succeeds: bool
) -> None:
    """Test a 502 or 504 without `Retry-After` is waited out a few times, each wait
    counting toward the wait limit, before it fails the check"""
    answers = iter([*(httpx.Response(status) for status in statuses), answer(1)])
    route = Route(lambda rows: next(answers))

    if succeeds:
        route.check([statvar("Variable1")])
    else:
        with pytest.raises(GraphCheckError):
            route.check([statvar("Variable1")])

    assert len(route.waits) == len(statuses) - (not succeeds)
    assert all(wait > 0 for wait in route.waits)


@pytest.mark.parametrize(
    ("handler", "requests"),
    [
        pytest.param(lambda rows: answer(1), 2, id="slow answers"),
        pytest.param(
            lambda rows: httpx.Response(503, headers={"Retry-After": "25"}),
            1,
            id="a wait past the limit",
        ),
    ],
)
def test_check_statvars_gives_up_past_the_time_limit(
    handler: Handler, requests: int
) -> None:
    """Test a check given a time limit fails once the time spent answering and
    waiting would pass it, and sends no request after it"""
    route = Route(handler, answer_seconds=20.0)

    with pytest.raises(GraphCheckError):
        route.check(
            [statvar(f"Variable{index}") for index in range(3)], time_limit=30.0
        )

    assert len(route.requests) == requests
    assert route.now <= 40.0


def test_check_statvars_caps_each_request_timeout_at_the_time_left() -> None:
    """Test every timeout of a request, connect, read, write and pool, is the time
    left when that is under the request timeout"""
    route = Route(lambda rows: answer(1), answer_seconds=20.0)

    with pytest.raises(GraphCheckError):
        route.check(
            [statvar(f"Variable{index}") for index in range(3)], time_limit=30.0
        )

    assert route.timeouts == [
        dict.fromkeys(("connect", "read", "write", "pool"), 30.0),
        dict.fromkeys(("connect", "read", "write", "pool"), 10.0),
    ]


def test_check_statvars_reports_a_request_timed_out_by_the_limit_as_the_limit() -> None:
    """Test a request that times out within the time left is a time limit failure,
    while one that times out with the full request timeout is unreachable"""

    def times_out(rows: list[dict[str, Any]]) -> httpx.Response:
        raise httpx.ReadTimeout("slow")

    capped = Route(times_out, answer_seconds=29.9)
    with pytest.raises(GraphCheckError, match="time limit"):
        capped.check([statvar("Variable1")], time_limit=30.0)
    uncapped = Route(times_out)
    with pytest.raises(GraphCheckError, match="unreachable"):
        uncapped.check([statvar("Variable1")], time_limit=3_000.0)


@pytest.mark.parametrize(
    ("retry_after", "waits"),
    [
        pytest.param(lambda: "0", 1.0, id="zero"),
        pytest.param(lambda: "Wed, 21 Oct 2015 07:28:00 GMT", 1.0, id="past date"),
        pytest.param(
            lambda: "Wed, 21 Oct 2015 07:28:00 -0000", 1.0, id="past date, no zone"
        ),
        pytest.param(
            # Dated when the route answers, as a slow suite can outlast a date
            # fixed at collection.
            lambda: format_datetime(
                datetime.now(UTC) + timedelta(seconds=30), usegmt=True
            ),
            30.0,
            id="future date",
        ),
    ],
)
def test_check_statvars_counts_each_retry_after_toward_the_wait_limit(
    retry_after: Callable[[], str], waits: float
) -> None:
    """Test a `Retry-After` in delta seconds or as an HTTP date is waited out, each
    wait counting at least a second, so a route asking for no wait still runs out
    the limit"""
    route = Route(
        lambda rows: httpx.Response(503, headers={"Retry-After": retry_after()})
    )

    with pytest.raises(GraphCheckError):
        route.check([statvar("Variable1")])

    assert route.waits[0] == pytest.approx(waits, abs=2.0)
    assert sum(route.waits) <= RETRY_WAIT_LIMIT_SECONDS


def unreachable(rows: list[dict[str, Any]]) -> httpx.Response:
    raise httpx.ConnectError("refused")


@pytest.mark.parametrize(
    "handler",
    [
        pytest.param(unreachable, id="unreachable"),
        pytest.param(answer(0), id="no row checked"),
        pytest.param(answer(2), id="more rows than sent"),
        pytest.param(answer(1, finding(1)), id="finding past the rows checked"),
        pytest.param(httpx.Response(503), id="503 without Retry-After"),
        pytest.param(httpx.Response(400, json={}), id="refusal naming no row"),
        pytest.param(
            httpx.Response(400, json={"pointer": "/rows/1/dcid"}),
            id="refusal naming a row not sent",
        ),
        pytest.param(httpx.Response(200, text="<html>"), id="200 not JSON"),
        pytest.param(httpx.Response(200, json=[]), id="200 not an object"),
        pytest.param(
            httpx.Response(200, json={"generation": "g1", "findings": []}),
            id="200 without rows_checked",
        ),
        pytest.param(
            httpx.Response(
                200,
                json={
                    "generation": "g1",
                    "rows_checked": 1,
                    "findings": [{"row": 0, "field": "dcid"}],
                },
            ),
            id="finding without its members",
        ),
        pytest.param(answer(1, finding(0, property=["aidType"])), id="list property"),
        pytest.param(answer(1, finding(0, field=["dcid"])), id="list field"),
        pytest.param(answer(1, finding(0, code=1)), id="number code"),
        pytest.param(answer(1, finding(0, severity=None)), id="null severity"),
    ],
)
def test_check_statvars_fails_on_an_answer_it_cannot_place(
    handler: Handler | httpx.Response,
) -> None:
    """Test an unreachable Graph API, or an answer that would leave rows unchecked,
    misplace a finding or carry a member of the wrong type, fails the check"""
    route = Route(
        (lambda rows: handler) if isinstance(handler, httpx.Response) else handler
    )

    with pytest.raises(GraphCheckError):
        route.check([statvar("Variable1")])


def test_open_client_posts_to_the_base_url_within_the_request_timeout() -> None:
    """Test the client the check opens sends to the Graph API's URL and gives
    each request the check's timeout"""
    sent: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return answer(1)

    with open_client(BASE_URL, transport=httpx.MockTransport(record)) as client:
        client.post(CHECK_PATH, json={"rows": []})

    assert [str(request.url) for request in sent] == [f"{BASE_URL}{CHECK_PATH}"]
    assert sent[0].extensions["timeout"]["read"] == REQUEST_TIMEOUT_SECONDS


def test_check_spec_fails_on_a_graph_api_url_httpx_rejects() -> None:
    """Test a base URL httpx refuses to build a request on fails the check"""
    with pytest.raises(GraphCheckError):
        check_spec(EXPORT_SPECS[0], [statvar("Variable1")], base_url="https://g\t/")


def test_check_statvars_skips_a_row_the_route_refuses() -> None:
    """Test a 400 naming one row reports that row and sends the rest again"""
    answers = iter(
        [
            httpx.Response(
                400,
                json={
                    "detail": "not finite",
                    "pointer": "/rows/1/constraint_properties",
                },
            ),
            answer(2, finding(1)),
        ]
    )
    route = Route(lambda rows: next(answers))

    check = route.check([statvar(f"Variable{index}") for index in range(1, 4)])

    assert [[row["dcid"] for row in rows] for rows in route.requests] == [
        ["Variable1", "Variable2", "Variable3"],
        ["Variable1", "Variable3"],
    ]
    assert [(r.statvar, r.pointer, r.detail) for r in check.refused] == [
        ("Variable2", "/rows/1/constraint_properties", "not finite")
    ]
    assert [f.statvar for f in check.findings] == ["Variable3"]


def test_check_statvars_gives_up_after_the_refused_row_limit() -> None:
    """Test a route refusing row after row fails the check once it has refused
    more rows than the limit, rather than sending the rest again for each"""
    route = Route(
        lambda rows: httpx.Response(400, json={"detail": "bad", "pointer": "/rows/0"})
    )

    with pytest.raises(GraphCheckError) as error:
        route.check([statvar(f"Variable{index}") for index in range(1_000)])

    assert len(route.requests) == REFUSED_ROW_LIMIT + 1
    assert len(error.value.refused) == REFUSED_ROW_LIMIT + 1


def test_check_statvars_counts_only_route_refusals_toward_the_refused_row_limit() -> (
    None
):
    """Test rows refused before sending leave the limit to the route's refusals, and
    every refused row names its index among the model's rows"""
    refusals = iter(
        [httpx.Response(400, json={"detail": "bad", "pointer": "/rows/1/stat_type"})]
    )
    route = Route(lambda rows: next(refusals, None) or answer(len(rows)))
    nan = float("nan")
    statvars = [
        *(statvar(f"Nan{index}", aid_type=nan) for index in range(REFUSED_ROW_LIMIT)),
        statvar("Variable1"),
        statvar("Variable2"),
    ]

    check = route.check(statvars)

    assert len(check.refused) == REFUSED_ROW_LIMIT + 1
    assert [(r.statvar, r.pointer) for r in check.refused][-1] == (
        "Variable2",
        f"/rows/{REFUSED_ROW_LIMIT + 1}/stat_type",
    )
    assert sum(r.before_sending for r in check.refused) == REFUSED_ROW_LIMIT
    assert "refused before sending" in str(check.refused[0])


@pytest.mark.parametrize("value", [float("nan"), float("inf")], ids=["nan", "inf"])
def test_check_statvars_refuses_a_row_with_a_number_json_cannot_carry(
    value: float,
) -> None:
    """Test a row whose cell is not a finite number is reported refused and the
    other rows are checked"""
    route = Route(lambda rows: answer(len(rows)))

    check = route.check([statvar("Variable1", aid_type=value), statvar("Variable2")])

    assert [[row["dcid"] for row in rows] for rows in route.requests] == [["Variable2"]]
    assert [(r.statvar, r.pointer) for r in check.refused] == [
        ("Variable1", "/rows/0/constraint_properties/aidType")
    ]


def test_check_statvars_drops_absent_node_findings_on_statvars_the_rows_define() -> (
    None
):
    """Test an absent-node finding on a denominator naming a StatVar the rows define
    is dropped, since the graph lacks it until ingestion, while other findings on
    such a value, and absent-node findings where a StatVar is no valid value, are
    kept"""
    route = Route(
        lambda rows: answer(
            2,
            *(
                finding(
                    0, field="measurement_denominator", value="Variable2", code=code
                )
                for code in ("node_not_held", "node_untyped", "node_provisional")
            ),
            finding(0, field="population_type", property=None, value="Variable2"),
            finding(0, field="measured_property", property=None, value="Variable2"),
            finding(0, value="Variable2"),
            finding(
                0,
                field="measurement_denominator",
                property=None,
                value="Missing1",
            ),
            finding(
                1,
                field="measurement_denominator",
                property=None,
                value="Variable1",
                code="definition_differs",
                severity="warning",
            ),
            finding(1, value="AidType9"),
        )
    )

    findings = route.check(
        [
            statvar("Variable1", measurement_denominator="dcid:Variable2"),
            statvar("Variable2"),
        ]
    ).findings

    assert [(f.statvar, f.field, f.code, f.value) for f in findings] == [
        ("Variable1", "population_type", "node_not_held", "Variable2"),
        ("Variable1", "measured_property", "node_not_held", "Variable2"),
        ("Variable1", "constraint_properties", "node_not_held", "Variable2"),
        ("Variable1", "measurement_denominator", "node_not_held", "Missing1"),
        ("Variable2", "measurement_denominator", "definition_differs", "Variable1"),
        ("Variable2", "constraint_properties", "node_not_held", "AidType9"),
    ]
