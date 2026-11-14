import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from dfo_pipelines.graph_check.client import (
    DEFAULT_WAIT_SECONDS,
    GATEWAY_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    CheckResult,
    FailedRow,
    StopReason,
    check_rows,
)
from dfo_pipelines.graph_check.rows import CheckRow
from tests.graph_check.tables import URL, answer, finding, refused

type Respond = Callable[[list[CheckRow]], httpx.Response]


def make_row(dcid: str) -> CheckRow:
    return CheckRow(
        dcid=dcid,
        population_type="FinancialFlow",
        measured_property="amount",
        stat_type=None,
        measurement_qualifier=None,
        measurement_denominator=None,
        constraint_properties={},
        observation_properties=[],
    )


def make_rows(count: int) -> list[CheckRow]:
    return [make_row(f"r{n}") for n in range(count)]


def answer_each(rows: list[CheckRow]) -> httpx.Response:
    """A 200 with one finding per row, whose value is the row's dcid"""
    return answer(
        rows, findings=[finding(n, value=row["dcid"]) for n, row in enumerate(rows)]
    )


class FakeRoute:
    """Answers each request with the next step of `script`, then with `respond`.

    Its clock advances `latency` seconds with each request and by every sleep. A
    request given less time than `latency` times out, as it does in httpx.
    """

    def __init__(
        self,
        *script: httpx.Response | Exception,
        respond: Respond = answer_each,
        latency: float = 0.0,
    ) -> None:
        self.script = list(script)
        self.respond = respond
        self.latency = latency
        self.now = 0.0
        self.requests: list[list[CheckRow]] = []
        self.timeouts: list[float] = []
        self.sleeps: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds

    def post(self, url: str, body: dict[str, Any], timeout: float) -> httpx.Response:
        assert url == URL
        self.requests.append(body["rows"])
        self.timeouts.append(timeout)
        self.now += self.latency
        if self.latency > timeout:
            raise httpx.ReadTimeout("timed out")
        if self.script:
            step = self.script.pop(0)
            if isinstance(step, Exception):
                raise step
            return step
        return self.respond(body["rows"])

    def check(self, rows: list[CheckRow], **kwargs: Any) -> CheckResult:
        return check_rows(
            rows,
            url=URL,
            post=self.post,
            sleep=self.sleep,
            clock=lambda: self.now,
            **kwargs,
        )

    def sent_dcids(self) -> list[list[str | None]]:
        return [[row["dcid"] for row in request] for request in self.requests]


def answered(result: CheckResult) -> list[int]:
    """The run's row of each finding, which `answer_each` gives every row one of"""
    return [finding["row"] for finding in result.findings]


def stop_reason(result: CheckResult) -> StopReason | None:
    return None if result.stopped is None else result.stopped.reason


def test_rows_go_in_chunks_and_every_answer_is_kept() -> None:
    route = FakeRoute()

    result = route.check(make_rows(5), chunk_size=2)

    assert [len(request) for request in route.requests] == [2, 2, 1]
    assert set(route.timeouts) == {REQUEST_TIMEOUT_SECONDS}
    assert answered(result) == [0, 1, 2, 3, 4]
    assert result.checked_rows == [0, 1, 2, 3, 4]
    assert result.generations == ["g1"]
    assert result.failed_rows == []


def test_a_partial_answer_resumes_at_its_next_row_and_maps_findings_by_row() -> None:
    def check_three(rows: list[CheckRow]) -> httpx.Response:
        checked = min(3, len(rows))
        return answer(
            rows,
            row_count=checked,
            findings=[finding(n, value=rows[n]["dcid"]) for n in range(checked)],
        )

    route = FakeRoute(respond=check_three)

    result = route.check(make_rows(8), chunk_size=5)

    assert route.sent_dcids() == [
        ["r0", "r1", "r2", "r3", "r4"],
        ["r3", "r4", "r5", "r6", "r7"],
        ["r6", "r7"],
    ]
    assert answered(result) == list(range(8))
    assert [finding["value"] for finding in result.findings] == [
        f"r{n}" for n in range(8)
    ]
    assert result.stopped is None


def test_a_400_naming_a_row_refuses_that_row_and_the_rest_go_on() -> None:
    def refuse_r2(rows: list[CheckRow]) -> httpx.Response:
        dcids = [row["dcid"] for row in rows]
        if "r2" in dcids:
            return refused(dcids.index("r2"), "a string past its bound")
        return answer_each(rows)

    route = FakeRoute(respond=refuse_r2)

    result = route.check(make_rows(5), chunk_size=4)

    assert route.sent_dcids() == [
        ["r0", "r1", "r2", "r3"],
        ["r0", "r1", "r3", "r4"],
    ]
    assert result.failed_rows == [FailedRow(row=2, detail="a string past its bound")]
    assert answered(result) == [0, 1, 3, 4]
    assert result.stopped is None


@pytest.mark.parametrize(
    ("refusal", "detail"),
    [
        (refused(None, "2,001 rows"), "answer 400, 2,001 rows"),
        (refused(2), "answer 400, bad"),
        (
            httpx.Response(400, json={"code": "request_invalid", "detail": "x"}),
            "answer 400, x",
        ),
        (
            httpx.Response(
                400,
                json={"code": "request_invalid", "detail": "x", "pointer": "/rows/a"},
            ),
            "answer 400, x",
        ),
        (
            httpx.Response(400, text="<html>Bad Request</html>"),
            "answer 400, no detail given",
        ),
    ],
    ids=[
        "whole list",
        "row not sent",
        "no pointer",
        "pointer not an index",
        "not JSON",
    ],
)
def test_a_400_naming_no_row_sent_stops_the_check_with_its_reason(
    refusal: httpx.Response, detail: str
) -> None:
    route = FakeRoute(answer_each(make_rows(2)), refusal)

    result = route.check(make_rows(4), chunk_size=2)

    assert len(route.requests) == 2
    assert stop_reason(result) is StopReason.REFUSED
    assert result.stopped is not None and result.stopped.detail == detail
    assert result.failed_rows == []
    assert answered(result) == [0, 1]


@pytest.mark.parametrize(
    ("busy", "waited"),
    [
        (httpx.Response(503, headers={"Retry-After": "7"}), [7.0]),
        (httpx.Response(503, headers={"Retry-After": "0"}), [1.0]),
        (httpx.Response(503, headers={"Retry-After": "-1"}), [DEFAULT_WAIT_SECONDS]),
    ],
    ids=["503 Retry-After", "no wait", "negative wait"],
)
def test_a_busy_answer_is_waited_out(busy: httpx.Response, waited: list[float]) -> None:
    route = FakeRoute(busy)

    result = route.check([make_row("a")])

    assert route.sleeps == waited
    assert answered(result) == [0]


def test_waiting_on_one_chunk_is_bounded() -> None:
    busy = httpx.Response(503, headers={"Retry-After": "25"})
    route = FakeRoute(busy, busy, busy, busy)

    result = route.check([make_row("a")])

    assert stop_reason(result) is StopReason.BUSY
    assert route.sleeps == [25.0, 25.0]


def test_a_gateway_failure_is_retried_a_bounded_number_of_times() -> None:
    gateway = httpx.Response(502, json={"error": "upstream", "detail": "x"})
    recovered = FakeRoute(gateway, httpx.Response(504))
    assert answered(recovered.check([make_row("a")])) == [0]

    persistent = FakeRoute(*[gateway] * (GATEWAY_RETRIES + 1))
    assert stop_reason(persistent.check([make_row("a")])) is StopReason.FAILED
    assert len(persistent.requests) == GATEWAY_RETRIES + 1


@pytest.mark.parametrize(
    ("failure", "reason"),
    [
        (httpx.ConnectError("refused"), StopReason.UNREACHABLE),
        (httpx.Response(500), StopReason.FAILED),
        (httpx.Response(403, text="forbidden"), StopReason.REFUSED),
        (
            httpx.Response(307, headers={"Location": "https://graph.test/check"}),
            StopReason.REDIRECTED,
        ),
        (httpx.Response(200, text="ok"), StopReason.UNEXPECTED_ANSWER),
    ],
    ids=["network", "500", "403", "redirect", "undocumented 200"],
)
def test_other_failures_stop_the_check_with_their_reason(
    failure: httpx.Response | Exception, reason: StopReason
) -> None:
    route = FakeRoute(failure)

    result = route.check([make_row("a")])

    assert stop_reason(result) is reason
    assert result.checked_count == 0


def documented(**changes: Any) -> dict[str, Any]:
    """A 200 body checking one of two rows sent, with `changes`; `...` drops one"""
    body = {
        "generation": "g1",
        "row_count": 1,
        "next_row": 1,
        "findings": [finding(0)],
    }
    return {key: value for key, value in (body | changes).items() if value != ...}


def first_of_two() -> httpx.Response:
    """An answer checking the first of two rows sent"""
    return httpx.Response(200, json=documented())


@pytest.mark.parametrize(
    "body",
    [
        [],
        documented(generation=...),
        documented(generation=3),
        documented(row_count=...),
        documented(row_count=True),
        documented(row_count=0),
        documented(row_count=3, next_row=None),
        documented(next_row=...),
        documented(next_row=None),
        documented(next_row=2),
        documented(row_count=2, next_row=2),
        documented(findings=None),
        documented(findings="none"),
        documented(findings=["b"]),
        documented(findings=[finding(1)]),
        documented(findings=[finding(-1)]),
        documented(findings=[finding(True)]),
        documented(findings=[finding(0, row=...)]),
        documented(findings=[finding(0, field=...)]),
        documented(findings=[finding(0, property=...)]),
        documented(findings=[finding(0, property=3)]),
        documented(findings=[finding(0, subject="field")]),
        documented(findings=[finding(0, value=...)]),
        documented(findings=[finding(0, served=...)]),
        documented(findings=[finding(0, code=1)]),
        documented(findings=[finding(0, severity="info")]),
    ],
    ids=[
        "not an object",
        "no generation",
        "generation not text",
        "no row count",
        "row count not a number",
        "row count zero",
        "row count past the rows sent",
        "no next row",
        "next row null on a partial answer",
        "next row past the rows checked",
        "next row on a full answer",
        "findings null",
        "findings text",
        "finding not an object",
        "finding about a row not checked",
        "finding row negative",
        "finding row not a number",
        "finding without row",
        "finding without field",
        "finding without property",
        "property not text",
        "unknown subject",
        "finding without value",
        "finding without served",
        "code not text",
        "unknown severity",
    ],
)
def test_an_answer_not_as_documented_stops_the_check_and_keeps_earlier_answers(
    body: Any,
) -> None:
    route = FakeRoute(first_of_two(), httpx.Response(200, json=body))

    result = route.check(make_rows(3), chunk_size=2)

    assert route.sent_dcids()[1] == ["r1", "r2"]
    assert stop_reason(result) is StopReason.UNEXPECTED_ANSWER
    assert answered(result) == [0]
    assert result.checked_count == 1


def test_an_answer_as_documented_is_kept() -> None:
    # The control for the cases above: the same exchange with `documented()`.
    route = FakeRoute(first_of_two(), httpx.Response(200, json=documented()))

    result = route.check(make_rows(3), chunk_size=2)

    assert stop_reason(result) is None
    assert result.checked_rows == [0, 1, 2]


def test_a_run_stops_at_its_request_budget_and_keeps_its_answers() -> None:
    route = FakeRoute()

    result = route.check(make_rows(5), chunk_size=1, max_requests=3)

    assert len(route.requests) == 3
    assert stop_reason(result) is StopReason.REQUEST_BUDGET
    assert result.stopped is not None
    assert result.stopped.message == "the check reached its limit of 3 requests"
    assert answered(result) == [0, 1, 2]


def test_a_run_stops_at_its_time_budget_and_keeps_its_answers() -> None:
    route = FakeRoute(latency=50.0)

    result = route.check(make_rows(5), chunk_size=1, max_seconds=120.0)

    assert stop_reason(result) is StopReason.TIME_BUDGET
    assert result.stopped is not None
    assert result.stopped.message == "the check reached its 2-minute limit"
    assert answered(result) == [0, 1]
    assert route.timeouts == [60.0, 60.0, 20.0]


def test_a_request_cut_short_by_the_time_budget_is_not_an_outage() -> None:
    route = FakeRoute(latency=7.0)

    result = route.check(make_rows(500), chunk_size=1, max_seconds=480.0)

    assert stop_reason(result) is StopReason.TIME_BUDGET
    assert result.checked_count == 68


def test_a_timeout_with_time_to_spare_is_an_outage() -> None:
    route = FakeRoute(httpx.ReadTimeout("timed out"))

    result = route.check([make_row("a")])

    assert stop_reason(result) is StopReason.UNREACHABLE


def test_a_connection_failure_near_the_deadline_is_still_an_outage() -> None:
    route = FakeRoute(httpx.ConnectError("refused"))

    result = route.check([make_row("a")], max_seconds=30.0)

    assert route.timeouts == [30.0]
    assert stop_reason(result) is StopReason.UNREACHABLE


def test_a_wait_that_would_pass_the_time_budget_stops_the_run_at_once() -> None:
    route = FakeRoute(httpx.Response(503, headers={"Retry-After": "30"}))

    result = route.check([make_row("a")], max_seconds=20.0)

    assert stop_reason(result) is StopReason.TIME_BUDGET
    assert route.sleeps == []


def test_a_run_sends_every_request_over_one_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clients: list[httpx.Client] = []
    real_client = httpx.Client

    def client(**kwargs: Any) -> httpx.Client:
        def respond(request: httpx.Request) -> httpx.Response:
            return answer_each(json.loads(request.content)["rows"])

        made = real_client(transport=httpx.MockTransport(respond), **kwargs)
        clients.append(made)
        return made

    monkeypatch.setattr(httpx, "Client", client)

    result = check_rows(make_rows(3), url=URL, chunk_size=1)

    assert answered(result) == [0, 1, 2]
    assert len(clients) == 1
    assert clients[0].is_closed
