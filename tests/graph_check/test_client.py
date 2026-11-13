from collections.abc import Callable
from typing import Any

import httpx
import pytest

from dfo_pipelines.graph_check.client import (
    GATEWAY_RETRIES,
    CheckResult,
    FailedRow,
    GraphCheckError,
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
    """Answers each request with the next step of `script`, then with `respond`"""

    def __init__(
        self, *script: httpx.Response | Exception, respond: Respond = answer
    ) -> None:
        self.script = list(script)
        self.respond = respond
        self.requests: list[list[CheckRow]] = []
        self.sleeps: list[float] = []

    def post(self, url: str, body: dict[str, Any]) -> httpx.Response:
        assert url == URL
        self.requests.append(body["rows"])
        if self.script:
            step = self.script.pop(0)
            if isinstance(step, Exception):
                raise step
            return step
        return self.respond(body["rows"])

    def check(self, rows: list[CheckRow], **kwargs: Any) -> CheckResult:
        return check_rows(
            rows, url=URL, post=self.post, sleep=self.sleeps.append, **kwargs
        )


def answered_refs(result: CheckResult) -> list[str]:
    return [finding["ref"] for finding in result.findings]


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


def test_a_400_that_is_not_a_request_problem_stops_the_check() -> None:
    route = FakeRoute(httpx.Response(400, json={"code": "bad_route", "detail": "x"}))

    with pytest.raises(GraphCheckError, match="400"):
        route.check([make_row("a"), make_row("b")])


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

    with pytest.raises(GraphCheckError, match="still busy"):
        route.check([make_row("a")])
    assert route.sleeps == [25.0, 25.0]


def test_a_gateway_failure_is_retried_a_bounded_number_of_times() -> None:
    gateway = httpx.Response(502, json={"error": "upstream", "detail": "x"})
    recovered = FakeRoute(gateway, httpx.Response(504))
    assert answered_refs(recovered.check([make_row("a")])) == ["a"]

    persistent = FakeRoute(*[gateway] * (GATEWAY_RETRIES + 1))
    with pytest.raises(GraphCheckError, match="502"):
        persistent.check([make_row("a")])
    assert len(persistent.requests) == GATEWAY_RETRIES + 1


@pytest.mark.parametrize(
    "failure",
    [
        httpx.ConnectError("refused"),
        httpx.Response(500),
        httpx.Response(200, text="ok"),
    ],
    ids=["network", "500", "undocumented 200"],
)
def test_other_failures_raise_a_named_error(
    failure: httpx.Response | Exception,
) -> None:
    route = FakeRoute(failure)

    with pytest.raises(GraphCheckError):
        route.check([make_row("a")])
