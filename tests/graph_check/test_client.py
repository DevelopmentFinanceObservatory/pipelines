from collections.abc import Callable
from typing import Any

import httpx
import pytest

from dfo_pipelines.graph_check.client import (
    GATEWAY_RETRIES,
    MAX_REQUESTS,
    CheckResult,
    FailedRow,
    StopReason,
    check_rows,
)
from dfo_pipelines.graph_check.rows import CheckRow

URL = "https://graph.test/check"

type Respond = Callable[[list[CheckRow]], httpx.Response]


def make_row(ref: str) -> CheckRow:
    return CheckRow(
        ref=ref,
        dcid=ref,
        population_type="FinancialFlow",
        measured_property="amount",
        stat_type=None,
        measurement_qualifier=None,
        measurement_denominator=None,
        constraint_properties={},
        observation_properties=[],
    )


def answer(rows: list[CheckRow], generation: str | None = "g1") -> httpx.Response:
    """A 200 with one finding per row, so a test can see which rows were answered"""
    return httpx.Response(
        200,
        json={
            "generation": generation,
            "row_count": len(rows),
            "findings": [
                {
                    "ref": row["ref"],
                    "field": "measured_property",
                    "value": "amount",
                    "code": "node_not_held",
                    "severity": "error",
                }
                for row in rows
            ],
        },
    )


class FakeRoute:
    """Answers each request with the next step of `script`, then with `respond`.

    Its clock advances `latency` seconds with each request and by every sleep.
    """

    def __init__(
        self,
        *script: httpx.Response | Exception,
        respond: Respond = answer,
        latency: float = 0.0,
    ) -> None:
        self.script = list(script)
        self.respond = respond
        self.latency = latency
        self.now = 0.0
        self.requests: list[list[CheckRow]] = []
        self.sleeps: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds

    def post(self, url: str, body: dict[str, Any]) -> httpx.Response:
        assert url == URL
        self.requests.append(body["rows"])
        self.now += self.latency
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


def answered_refs(result: CheckResult) -> list[str]:
    return [finding["ref"] for finding in result.findings]


def stop_reason(result: CheckResult) -> StopReason | None:
    return None if result.stopped is None else result.stopped.reason


def test_rows_go_in_chunks_and_every_answer_is_kept() -> None:
    route = FakeRoute()
    rows = [make_row(ref) for ref in "abcde"]

    result = route.check(rows, chunk_size=2)

    assert [len(request) for request in route.requests] == [2, 2, 1]
    assert answered_refs(result) == list("abcde")
    assert result.generations == ["g1"]
    assert result.failed_rows == []


def test_a_refused_chunk_halves_until_the_refused_row_stands_alone() -> None:
    def refuse_c(rows: list[CheckRow]) -> httpx.Response:
        if any(row["ref"] == "c" for row in rows):
            return httpx.Response(
                400, json={"code": "request_invalid", "detail": "too many ids"}
            )
        return answer(rows)

    route = FakeRoute(respond=refuse_c)

    result = route.check([make_row(ref) for ref in "abcde"])

    assert result.failed_rows == [FailedRow(ref="c", detail="too many ids")]
    assert sorted(answered_refs(result)) == ["a", "b", "d", "e"]


@pytest.mark.parametrize(
    "refusal",
    [
        httpx.Response(400, json={"code": "bad_route", "detail": "x"}),
        httpx.Response(400, json={"detail": "x"}),
        httpx.Response(400, text="<html>Bad Request</html>"),
    ],
    ids=["another code", "no code", "not JSON"],
)
def test_a_400_that_is_not_the_routes_refusal_of_rows_stops_the_check(
    refusal: httpx.Response,
) -> None:
    route = FakeRoute(refusal)

    result = route.check([make_row("a"), make_row("b")])

    assert len(route.requests) == 1
    assert stop_reason(result) is StopReason.REFUSED
    assert result.failed_rows == []
    assert result.stopped is not None and "html" not in result.stopped.detail


@pytest.mark.parametrize(
    ("busy", "waited"),
    [
        (httpx.Response(503, headers={"Retry-After": "7"}), [7.0]),
        (httpx.Response(429, json={"error": "rate", "retry": 3, "detail": "x"}), [3.0]),
    ],
    ids=["503 Retry-After", "429 retry"],
)
def test_a_busy_answer_is_waited_out(busy: httpx.Response, waited: list[float]) -> None:
    route = FakeRoute(busy)

    result = route.check([make_row("a")])

    assert route.sleeps == waited
    assert answered_refs(result) == ["a"]


def test_waiting_on_one_chunk_is_bounded() -> None:
    busy = httpx.Response(503, headers={"Retry-After": "25"})
    route = FakeRoute(busy, busy, busy, busy)

    result = route.check([make_row("a")])

    assert stop_reason(result) is StopReason.BUSY
    assert route.sleeps == [25.0, 25.0]


def test_a_gateway_failure_is_retried_a_bounded_number_of_times() -> None:
    gateway = httpx.Response(502, json={"error": "upstream", "detail": "x"})
    recovered = FakeRoute(gateway, httpx.Response(504))
    assert answered_refs(recovered.check([make_row("a")])) == ["a"]

    persistent = FakeRoute(*[gateway] * (GATEWAY_RETRIES + 1))
    assert stop_reason(persistent.check([make_row("a")])) is StopReason.FAILED
    assert len(persistent.requests) == GATEWAY_RETRIES + 1


@pytest.mark.parametrize(
    ("failure", "reason"),
    [
        (httpx.ConnectError("refused"), StopReason.UNREACHABLE),
        (httpx.Response(500), StopReason.FAILED),
        (httpx.Response(403, text="forbidden"), StopReason.REFUSED),
        (httpx.Response(200, text="ok"), StopReason.UNEXPECTED_ANSWER),
    ],
    ids=["network", "500", "403", "undocumented 200"],
)
def test_other_failures_stop_the_check_with_their_reason(
    failure: httpx.Response | Exception, reason: StopReason
) -> None:
    route = FakeRoute(failure)

    result = route.check([make_row("a")])

    assert stop_reason(result) is reason
    assert result.checked_count == 0


def finding_for(ref: str, **changes: Any) -> dict[str, Any]:
    finding = {
        "ref": ref,
        "field": "measured_property",
        "value": "amount",
        "code": "node_not_held",
        "severity": "error",
    }
    return {key: value for key, value in (finding | changes).items() if value != ...}


@pytest.mark.parametrize(
    "body",
    [
        [],
        {"row_count": 1, "findings": []},
        {"generation": 3, "row_count": 1, "findings": []},
        {"generation": "g1", "findings": []},
        {"generation": "g1", "row_count": True, "findings": []},
        {"generation": "g1", "row_count": 0, "findings": []},
        {"generation": "g1", "row_count": 2, "findings": []},
        {"generation": "g1", "row_count": 1, "findings": None},
        {"generation": "g1", "row_count": 1, "findings": "none"},
        {"generation": "g1", "row_count": 1, "findings": ["b"]},
        {"generation": "g1", "row_count": 1, "findings": [finding_for("z")]},
        {"generation": "g1", "row_count": 1, "findings": [finding_for("b", field=...)]},
        {"generation": "g1", "row_count": 1, "findings": [finding_for("b", value=...)]},
        {"generation": "g1", "row_count": 1, "findings": [finding_for("b", code=1)]},
        {
            "generation": "g1",
            "row_count": 1,
            "findings": [finding_for("b", severity="info")],
        },
    ],
    ids=[
        "not an object",
        "no generation",
        "generation not text",
        "no row count",
        "row count not a number",
        "row count short",
        "row count long",
        "findings null",
        "findings text",
        "finding not an object",
        "finding about a row not sent",
        "finding without field",
        "finding without value",
        "code not text",
        "unknown severity",
    ],
)
def test_an_answer_not_as_documented_stops_the_check_and_keeps_earlier_answers(
    body: Any,
) -> None:
    route = FakeRoute(answer([make_row("a")]), httpx.Response(200, json=body))

    result = route.check([make_row("a"), make_row("b")], chunk_size=1)

    assert stop_reason(result) is StopReason.UNEXPECTED_ANSWER
    assert answered_refs(result) == ["a"]
    assert result.checked_count == 1


def capped_route(cap: int) -> Respond:
    """A route that refuses a chunk whose rows name more than `cap` unknown ids.

    Every population type starting `Unknown` is one the graph does not hold.
    """

    def respond(rows: list[CheckRow]) -> httpx.Response:
        unknown = {
            row["population_type"]
            for row in rows
            if str(row["population_type"]).startswith("Unknown")
        }
        if len(unknown) > cap:
            return httpx.Response(
                400, json={"code": "request_invalid", "detail": "too many ids"}
            )
        return httpx.Response(
            200, json={"generation": "g1", "row_count": len(rows), "findings": []}
        )

    return respond


def test_a_large_table_costs_about_what_its_unknown_ids_require() -> None:
    rows = [
        make_row(f"r{n}") | {"population_type": f"Unknown{n}"} for n in range(5_000)
    ]
    route = FakeRoute(respond=capped_route(41))

    result = route.check(rows)

    # 5,000 unknown ids at 41 a request need 122 requests.
    assert result.checked_count == 5_000
    assert result.stopped is None
    assert len(route.requests) <= 150


def test_the_chunk_size_grows_back_once_rows_name_known_ids() -> None:
    unknown = [
        make_row(f"u{n}") | {"population_type": f"Unknown{n}"} for n in range(200)
    ]
    known = [make_row(f"k{n}") for n in range(5_000)]
    route = FakeRoute(respond=capped_route(41))

    result = route.check(unknown + known)

    assert result.checked_count == 5_200
    assert max(len(request) for request in route.requests[-5:]) == 500
    assert len(route.requests) <= 60


def test_a_run_stops_at_its_request_budget_and_keeps_its_answers() -> None:
    route = FakeRoute()

    result = route.check(
        [make_row(ref) for ref in "abcde"], chunk_size=1, max_requests=3
    )

    assert len(route.requests) == 3
    assert stop_reason(result) is StopReason.REQUEST_BUDGET
    assert answered_refs(result) == ["a", "b", "c"]


def test_a_run_stops_at_its_time_budget_and_keeps_its_answers() -> None:
    route = FakeRoute(latency=10.0)

    result = route.check(
        [make_row(ref) for ref in "abcde"], chunk_size=1, max_seconds=25.0
    )

    assert stop_reason(result) is StopReason.TIME_BUDGET
    assert answered_refs(result) == ["a", "b", "c"]


def test_a_wait_that_would_pass_the_time_budget_stops_the_run_at_once() -> None:
    route = FakeRoute(httpx.Response(503, headers={"Retry-After": "30"}))

    result = route.check([make_row("a")], max_seconds=20.0)

    assert stop_reason(result) is StopReason.TIME_BUDGET
    assert route.sleeps == []


def test_the_request_budget_fits_the_proxy_allowance_within_the_time_budget() -> None:
    from dfo_pipelines.graph_check.client import MAX_RUN_SECONDS

    assert MAX_REQUESTS <= 120 * MAX_RUN_SECONDS / 60
