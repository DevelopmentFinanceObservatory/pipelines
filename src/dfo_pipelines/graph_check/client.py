"""Send check rows to the Graph API's check route and gather its findings.

The route answers each request with the findings for its rows. It refuses a
request whose rows name more unknown ids than it reads in one check with a 400
problem document whose `code` is `request_invalid`; the client then sends fewer
rows at a time until a refused row stands alone, and records that row as failed.
The chunk size adapts in both directions: a refusal shrinks it for the chunks that
follow, and each answered chunk doubles it again, up to the starting size but
below the smallest size the route refused, until `CEILING_PROBE_AFTER` chunks in a
row are answered and that size is tried once more.

A busy route answers 503, and the website proxy in front of it answers 429 once a
caller passes its allowance; both say how long to wait. The proxy answers 502 or
504 when the route behind it fails, which is worth a few retries.

A whole run is bounded by `MAX_REQUESTS` requests and `MAX_RUN_SECONDS` seconds.
Any failure, a spent budget included, stops the run and keeps what it gathered.
"""

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

import httpx

from dfo_pipelines.graph_check.rows import CheckRow

# Sends a JSON body to a URL and returns the response.
type Post = Callable[[str, dict[str, Any]], httpx.Response]

# Rows sent in the first request. The route takes up to 2,000, but a chunk of rows
# naming many ids the graph does not hold is refused and sent again in smaller
# chunks, so a smaller start wastes fewer calls.
CHUNK_SIZE = 500

# Answered chunks in a row after which the smallest refused size is tried again,
# so a table whose later rows name fewer unknown ids is sent in larger chunks.
CEILING_PROBE_AFTER = 16

# The most requests one run sends, retries included. The proxy allows 120 calls a
# minute, so 900 calls take at least 7.5 minutes, close to `MAX_RUN_SECONDS`.
MAX_REQUESTS = 900

# The longest a run may take before it stops, within the CI step's 10-minute
# timeout, which also covers starting Python and reading the tables.
MAX_RUN_SECONDS = 8 * 60.0

# The longest time spent waiting on 503 and 429 answers for one chunk.
MAX_WAIT_SECONDS = 60.0

# The wait when a 503 or 429 answer names none.
DEFAULT_WAIT_SECONDS = 5.0

# Retries of a chunk answered 502 or 504, and the wait before each.
GATEWAY_RETRIES = 3
GATEWAY_RETRY_SECONDS = 2.0

# The time one request may take.
REQUEST_TIMEOUT_SECONDS = 60.0

# The problem code of the route's refusal of the rows it was sent.
ROWS_REFUSED_CODE = "request_invalid"

SEVERITIES = ("error", "warning")


class StopReason(StrEnum):
    """Why a run stopped before every row was answered."""

    UNREACHABLE = "the check route could not be reached"
    BUSY = "the check route stayed busy"
    FAILED = "the check route failed"
    REFUSED = "the request was refused"
    UNEXPECTED_ANSWER = "the check route gave an answer the check cannot read"
    TIME_BUDGET = f"the check reached its {MAX_RUN_SECONDS / 60:g}-minute limit"
    REQUEST_BUDGET = f"the check reached its limit of {MAX_REQUESTS:,} requests"


class GraphCheckError(Exception):
    """The check route could not be used, with the reason and its detail."""

    def __init__(self, reason: StopReason, detail: str) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True, kw_only=True)
class FailedRow:
    """A row the route refused on its own, with the route's reason."""

    ref: str
    detail: str


@dataclass(kw_only=True)
class CheckResult:
    """What the route answered for the rows sent.

    Attributes:
        generations: The graph generations the answers were checked against, in
            the order first seen; None when the route named none.
        findings: Every finding, each with `ref`, `field`, `value`, `code` and
            `severity`.
        checked_count: The rows the route answered.
        failed_rows: The rows the route refused even when sent alone.
        stopped: Why the run stopped before every row was answered or refused,
            or None when it did not.
    """

    generations: list[str | None] = field(default_factory=list)
    findings: list[dict[str, Any]] = field(default_factory=list)
    checked_count: int = 0
    failed_rows: list[FailedRow] = field(default_factory=list)
    stopped: GraphCheckError | None = None


def post_json(url: str, body: dict[str, Any]) -> httpx.Response:
    """POST `body` as JSON to `url` with httpx"""
    return httpx.post(url, json=body, timeout=REQUEST_TIMEOUT_SECONDS)


def check_rows(
    rows: Sequence[CheckRow],
    *,
    url: str,
    post: Post = post_json,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    chunk_size: int = CHUNK_SIZE,
    max_requests: int = MAX_REQUESTS,
    max_seconds: float = MAX_RUN_SECONDS,
) -> CheckResult:
    """Send `rows` to the check route in chunks and gather the answers.

    Args:
        rows: The rows to check.
        url: The check route's URL.
        post: Sends one request; `post_json` unless a test passes another.
        sleep: Waits the given seconds; `time.sleep` unless a test passes another.
        clock: Reads the time in seconds; `time.monotonic` unless a test passes
            another.
        chunk_size: The most rows one request carries.
        max_requests: The most requests the run sends.
        max_seconds: The longest the run may take.

    Returns:
        The findings, the rows the route refused, the graph generations, and why
        the run stopped early if it did. A run stops at the first request that
        fails on the network, waits more than `MAX_WAIT_SECONDS` on 503 and 429
        answers, meets a 502 or 504 past `GATEWAY_RETRIES`, gets an answer it
        cannot read, or would pass either budget.
    """
    result = CheckResult()
    sender = _Sender(
        url=url,
        post=post,
        sleep=sleep,
        clock=clock,
        requests_left=max_requests,
        deadline=clock() + max_seconds,
    )
    sizes = _ChunkSizes(chunk_size)
    start = 0
    try:
        while start < len(rows):
            chunk = list(rows[start : start + sizes.size])
            response = sender.send(chunk)
            if response.status_code == 400:
                if len(chunk) > 1:
                    sizes.refused(len(chunk))
                    continue
                result.failed_rows.append(
                    FailedRow(ref=chunk[0]["ref"], detail=_detail(response))
                )
            else:
                _keep_answer(response, chunk, result)
                sizes.answered(len(chunk))
            start += len(chunk)
    except GraphCheckError as error:
        result.stopped = error
    return result


class _ChunkSizes:
    """The number of rows to send next, adapted to the route's refusals"""

    def __init__(self, start: int) -> None:
        self.start = start
        self.size = start
        self.ceiling: int | None = None
        self.largest_answered = 0
        self.answered_in_a_row = 0

    def refused(self, sent: int) -> None:
        """Send fewer than `sent` rows: halfway down to the largest size answered"""
        self.ceiling = sent
        self.answered_in_a_row = 0
        if self.largest_answered >= sent:
            self.largest_answered = 0
        self.size = max(1, (self.largest_answered + sent) // 2)

    def answered(self, sent: int) -> None:
        """Double the size, below the smallest size refused until it is due a retry"""
        self.largest_answered = max(self.largest_answered, sent)
        self.answered_in_a_row += 1
        if self.ceiling is not None and sent >= self.ceiling:
            self.ceiling = None
        size = min(2 * sent, self.start)
        if self.ceiling is not None:
            if self.answered_in_a_row >= CEILING_PROBE_AFTER:
                self.answered_in_a_row = 0
                size = min(size, self.ceiling)
            else:
                size = min(size, self.ceiling - 1)
        self.size = size


@dataclass(kw_only=True)
class _Sender:
    """Posts chunks within the run's request and time budgets"""

    url: str
    post: Post
    sleep: Callable[[float], None]
    clock: Callable[[], float]
    requests_left: int
    deadline: float

    def send(self, rows: list[CheckRow]) -> httpx.Response:
        """Post one chunk until it answers 200 or the route refuses its rows"""
        waited = 0.0
        gateway_failures = 0
        while True:
            response = self._post_once(rows)
            status = response.status_code
            if status == 200:
                return response
            if status == 400:
                if _code(response) == ROWS_REFUSED_CODE:
                    return response
                raise GraphCheckError(
                    StopReason.REFUSED, f"answer 400, {_detail(response)}"
                )
            if status in (429, 503):
                wait = _wait_seconds(response)
                if waited + wait > MAX_WAIT_SECONDS:
                    raise GraphCheckError(
                        StopReason.BUSY,
                        f"still answering {status} after {waited:g} seconds",
                    )
                self._sleep(wait)
                waited += wait
                continue
            if status in (502, 504) and gateway_failures < GATEWAY_RETRIES:
                gateway_failures += 1
                self._sleep(GATEWAY_RETRY_SECONDS)
                continue
            reason = StopReason.FAILED if status >= 500 else StopReason.REFUSED
            raise GraphCheckError(reason, f"answer {status}, {_detail(response)}")

    def _post_once(self, rows: list[CheckRow]) -> httpx.Response:
        """One request, if both budgets allow it"""
        if self.requests_left <= 0:
            raise GraphCheckError(StopReason.REQUEST_BUDGET, "no requests left")
        if self.clock() >= self.deadline:
            raise GraphCheckError(StopReason.TIME_BUDGET, "no time left")
        self.requests_left -= 1
        try:
            return self.post(self.url, {"rows": rows})
        except httpx.HTTPError as error:
            raise GraphCheckError(
                StopReason.UNREACHABLE, type(error).__name__
            ) from error

    def _sleep(self, seconds: float) -> None:
        """Wait, unless the wait would end past the deadline"""
        if self.clock() + seconds >= self.deadline:
            raise GraphCheckError(
                StopReason.TIME_BUDGET, f"a {seconds:g}-second wait would pass it"
            )
        self.sleep(seconds)


def _keep_answer(
    response: httpx.Response, rows: list[CheckRow], result: CheckResult
) -> None:
    """Add a 200 answer to `result` once every part of it is as the route documents"""
    try:
        answer = response.json()
    except ValueError:
        answer = None
    if not isinstance(answer, dict):
        raise _unexpected("a body that is not a JSON object")
    generation = answer.get("generation")
    if "generation" not in answer or not isinstance(generation, str | None):
        raise _unexpected("no generation")
    row_count = answer.get("row_count")
    if isinstance(row_count, bool) or not isinstance(row_count, int):
        raise _unexpected("no row count")
    if row_count != len(rows):
        raise _unexpected(f"a count of {row_count:,} checked for {len(rows):,} sent")
    findings = answer.get("findings")
    if not isinstance(findings, list):
        raise _unexpected("no list of findings")
    refs = {row["ref"] for row in rows}
    if not all(_is_finding(finding, refs) for finding in findings):
        raise _unexpected("a finding that does not match the rows sent")

    if generation not in result.generations:
        result.generations.append(generation)
    result.findings.extend(findings)
    result.checked_count += len(rows)


def _is_finding(finding: Any, refs: set[str]) -> bool:
    """Whether `finding` has the members the route documents, about a row sent"""
    return (
        isinstance(finding, dict)
        and finding.get("ref") in refs
        and isinstance(finding.get("field"), str)
        and isinstance(finding.get("code"), str)
        and finding.get("severity") in SEVERITIES
        and "value" in finding
    )


def _unexpected(detail: str) -> GraphCheckError:
    """The error for a 200 answer the route does not document"""
    return GraphCheckError(StopReason.UNEXPECTED_ANSWER, f"answer 200 with {detail}")


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
    """The reason a problem document gives, or a note that there is none"""
    body = _body(response)
    detail = body.get("detail")
    if isinstance(body.get("code"), str) and isinstance(detail, str) and detail:
        return detail
    return "no detail given"


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
