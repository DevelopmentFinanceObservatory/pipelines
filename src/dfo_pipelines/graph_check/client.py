"""Send check rows to the Graph API's check route and gather its findings.

The route answers each request with the findings for its rows. A request whose
rows ask too much of the route answers 400, so a chunk that does is split in two
until the row at fault stands alone and is recorded as failed. A busy route
answers 503, and the website proxy in front of it answers 429 once a caller
passes its allowance; both say how long to wait. The proxy answers 502 or 504
when the route behind it fails, which is worth a few retries.
"""

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

import httpx

from dfo_pipelines.graph_check.rows import CheckRow

type Post = Callable[[str, dict[str, Any]], httpx.Response]
"""Sends a JSON body to a URL and returns the response."""

# Rows sent in one request. The route takes up to 2,000, but a chunk of rows naming
# many ids the graph does not hold answers 400 and is halved, so a smaller first
# chunk wastes fewer calls.
CHUNK_SIZE = 500

# The longest time spent waiting on 503 and 429 answers for one chunk.
MAX_WAIT_SECONDS = 60.0

# The wait when a 503 or 429 answer names none.
DEFAULT_WAIT_SECONDS = 5.0

# Retries of a chunk answered 502 or 504, and the wait before each.
GATEWAY_RETRIES = 3
GATEWAY_RETRY_SECONDS = 2.0

# The time one request may take.
REQUEST_TIMEOUT_SECONDS = 60.0


class GraphCheckError(Exception):
    """The check route could not be reached, or answered in a way the client cannot use."""


@dataclass(frozen=True, kw_only=True)
class FailedRow:
    """A row the route refused on its own, with the route's reason."""

    ref: str
    detail: str


@dataclass(kw_only=True)
class CheckResult:
    """What the route answered for every row sent.

    Attributes:
        generations: The graph generations the answers were checked against, in
            the order first seen; None when the route named none.
        findings: Every finding, as the route returned it.
        failed_rows: The rows the route refused even when sent alone.
    """

    generations: list[str | None] = field(default_factory=list)
    findings: list[dict[str, Any]] = field(default_factory=list)
    failed_rows: list[FailedRow] = field(default_factory=list)


def post_json(url: str, body: dict[str, Any]) -> httpx.Response:
    """POST `body` as JSON to `url` with httpx"""
    return httpx.post(url, json=body, timeout=REQUEST_TIMEOUT_SECONDS)


def check_rows(
    rows: Sequence[CheckRow],
    *,
    url: str,
    post: Post = post_json,
    sleep: Callable[[float], None] = time.sleep,
    chunk_size: int = CHUNK_SIZE,
) -> CheckResult:
    """Send `rows` to the check route in chunks and gather the answers.

    Args:
        rows: The rows to check.
        url: The check route's URL.
        post: Sends one request; `post_json` unless a test passes another.
        sleep: Waits the given seconds; `time.sleep` unless a test passes another.
        chunk_size: The most rows one request carries.

    Returns:
        The findings, the rows the route refused and the graph generations.

    Raises:
        GraphCheckError: If a request fails on the network, a chunk waits more than
            `MAX_WAIT_SECONDS` on 503 and 429 answers, a 502 or 504 persists past
            `GATEWAY_RETRIES`, or the route answers anything else it should not.
    """
    result = CheckResult()
    for start in range(0, len(rows), chunk_size):
        _check_chunk(
            list(rows[start : start + chunk_size]),
            url=url,
            post=post,
            sleep=sleep,
            result=result,
        )
    return result


def _check_chunk(
    rows: list[CheckRow],
    *,
    url: str,
    post: Post,
    sleep: Callable[[float], None],
    result: CheckResult,
) -> None:
    """Check one chunk, halving it on a 400 until the refused rows stand alone"""
    response = _send(rows, url=url, post=post, sleep=sleep)
    if response.status_code == 400:
        if len(rows) == 1:
            result.failed_rows.append(
                FailedRow(ref=rows[0]["ref"], detail=_detail(response))
            )
            return
        middle = len(rows) // 2
        for half in (rows[:middle], rows[middle:]):
            _check_chunk(half, url=url, post=post, sleep=sleep, result=result)
        return

    try:
        answer = response.json()
        generation = answer["generation"]
        findings = answer["findings"]
    except (ValueError, KeyError, TypeError) as error:
        raise GraphCheckError(
            f"the check route answered 200 with a body it does not document: {error}"
        ) from error
    if generation not in result.generations:
        result.generations.append(generation)
    result.findings.extend(findings)


def _send(
    rows: list[CheckRow],
    *,
    url: str,
    post: Post,
    sleep: Callable[[float], None],
) -> httpx.Response:
    """Post one chunk until it answers 200 or 400, waiting and retrying as told"""
    waited = 0.0
    gateway_failures = 0
    while True:
        try:
            response = post(url, {"rows": rows})
        except httpx.HTTPError as error:
            raise GraphCheckError(f"the request failed: {error}") from error

        status = response.status_code
        if status in (200, 400):
            if status == 400 and _code(response) not in (None, "request_invalid"):
                raise GraphCheckError(
                    f"the check route answered 400: {_detail(response)}"
                )
            return response
        if status in (429, 503):
            wait = _wait_seconds(response)
            if waited + wait > MAX_WAIT_SECONDS:
                raise GraphCheckError(
                    f"the check route was still busy after {waited:g} seconds "
                    f"(last answer {status}: {_detail(response)})"
                )
            waited += wait
            sleep(wait)
            continue
        if status in (502, 504) and gateway_failures < GATEWAY_RETRIES:
            gateway_failures += 1
            sleep(GATEWAY_RETRY_SECONDS)
            continue
        raise GraphCheckError(f"the check route answered {status}: {_detail(response)}")


def _body(response: httpx.Response) -> dict[str, Any]:
    """The response's JSON object, or an empty one when it has none"""
    try:
        body = response.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _code(response: httpx.Response) -> str | None:
    """The problem code a refusal names, if any"""
    code = _body(response).get("code")
    return code if isinstance(code, str) else None


def _detail(response: httpx.Response) -> str:
    """The reason a response gives, or the start of its text"""
    detail = _body(response).get("detail")
    if isinstance(detail, str) and detail:
        return detail
    return response.text[:200] or "no detail given"


def _wait_seconds(response: httpx.Response) -> float:
    """The seconds a 503 or 429 answer asks the caller to wait"""
    for value in (response.headers.get("Retry-After"), _body(response).get("retry")):
        if isinstance(value, bool) or not isinstance(value, str | int | float):
            continue
        try:
            seconds = float(value)
        except ValueError:
            continue
        if seconds >= 0:
            return seconds
    return DEFAULT_WAIT_SECONDS
