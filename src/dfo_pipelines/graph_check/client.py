"""Send check rows to the Graph API's check route and gather its findings.

The route checks the rows of a request in order and answers for as many as it
can read and report on in one check, at least one. Its answer counts the rows
checked, `rows_checked`, and each finding names its row by index in the request.
When fewer rows were checked than sent, the client sends the rest in the next
request.

The route refuses a request with a 400 problem document. When the document's
`pointer` names one row (`/rows/37/constraint_properties`), the client records
that row as refused with the route's reason and sends the rest without it. Any
other 400 stops the run.

A busy route answers 503, and the website proxy in front of it answers 429 once a
caller passes its allowance, and both say how long to wait. A 503 names its cause
in `cause.code`, which the stop reason repeats when the wait budget runs out. The
proxy answers 502 or 504 when the route behind it fails, which is worth a few
retries. A problem that says `retryable: false`, and a 500, stop the run at once.

A whole run is bounded by `MAX_REQUESTS` requests and `MAX_RUN_SECONDS` seconds
unless it is given other budgets. Any failure, a spent budget included, stops the
run and keeps what it gathered.
"""

import re
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

import httpx

from dfo_pipelines.graph_check.rows import CheckRow

# Sends a JSON body to a URL within a timeout in seconds and returns the response.
type Post = Callable[[str, dict[str, Any], float], httpx.Response]

# The most rows the route takes in one request.
CHUNK_SIZE = 2_000

# The most requests one run sends, retries included. Every answer settles at least
# one row, so the run always moves on, and the proxy's 120 calls a minute let 960
# calls through in `MAX_RUN_SECONDS`.
MAX_REQUESTS = 960

# The longest a run may take before it stops, within the CI step's 10-minute
# timeout, which also covers starting Python and reading the tables.
MAX_RUN_SECONDS = 8 * 60.0

# The longest time spent waiting on 503 and 429 answers for one chunk.
MAX_WAIT_SECONDS = 60.0

# The wait when a 503 or 429 answer names none.
DEFAULT_WAIT_SECONDS = 5.0

# The shortest wait on a 503 or 429 answer, whatever wait it names.
MIN_WAIT_SECONDS = 1.0

# Retries of a chunk answered 502 or 504, and the wait before each.
GATEWAY_RETRIES = 3
GATEWAY_RETRY_SECONDS = 2.0

# The longest time one request may take, cut to the time the run has left.
REQUEST_TIMEOUT_SECONDS = 60.0

SEVERITIES = ("error", "warning")

# What a finding judges: a constraint or observation property itself, or a value.
SUBJECTS = ("property", "value")

# What a 503 answer's `cause.code` says about why the route could not take the
# check, for the stop reason once the wait budget is spent.
BUSY_CAUSES = {
    "schema_building": "the check route was still building the schema",
    "check_busy": "the check route stayed busy with other checks",
}

# A problem pointer into one row of the request, which holds the row's index.
_ROW_POINTER = re.compile(r"^/rows/(\d+)(?:/|$)")


class StopReason(StrEnum):
    """Why a run stopped before every row was answered."""

    UNREACHABLE = "the check route could not be reached"
    BUSY = "the check route stayed busy"
    NOT_RETRYABLE = "the check route reported a failure it does not expect to clear"
    FAILED = "the check route failed"
    REFUSED = "the request was refused"
    REDIRECTED = "the check route answered with a redirect"
    UNEXPECTED_ANSWER = "the check route gave an answer the check cannot read"
    TIME_BUDGET = "the check reached its time limit"
    REQUEST_BUDGET = "the check reached its request limit"


class GraphCheckError(Exception):
    """The check route could not be used, with the reason and its detail.

    Attributes:
        reason: Why the run stopped.
        detail: What the route answered, or what failed.
        message: The reason as the report states it, with the budget a run
            reached named in it.
    """

    def __init__(
        self, reason: StopReason, detail: str, *, message: str | None = None
    ) -> None:
        self.reason = reason
        self.detail = detail
        self.message = str(reason) if message is None else message
        super().__init__(f"{self.message}: {detail}")


@dataclass(frozen=True, kw_only=True)
class FailedRow:
    """A row the route refused, by its index in the run's rows, with the reason."""

    row: int
    detail: str


@dataclass(kw_only=True)
class CheckResult:
    """What the route answered for the rows sent.

    Attributes:
        generations: The graph generations the answers were checked against, in
            the order first seen, or None when the route named none.
        findings: Every finding, as the route documents it, with `row` the
            index in the run's rows.
        checked_rows: The index of each row the route checked, in order sent.
        failed_rows: The rows the route refused.
        stopped: Why the run stopped before every row was answered or refused,
            or None when it did not.
    """

    generations: list[str | None] = field(default_factory=list)
    findings: list[dict[str, Any]] = field(default_factory=list)
    checked_rows: list[int] = field(default_factory=list)
    failed_rows: list[FailedRow] = field(default_factory=list)
    stopped: GraphCheckError | None = None

    @property
    def checked_count(self) -> int:
        """The rows the route checked"""
        return len(self.checked_rows)


@contextmanager
def _httpx_post() -> Iterator[Post]:
    """A `Post` sending every request of a run over one httpx client"""
    with httpx.Client() as client:

        def post(url: str, body: dict[str, Any], timeout: float) -> httpx.Response:
            return client.post(url, json=body, timeout=timeout)

        yield post


def check_rows(
    rows: Sequence[CheckRow],
    *,
    url: str,
    post: Post | None = None,
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
        post: Sends one request. Defaults to one httpx client for the run, which
            keeps its connection to the route open between requests.
        sleep: Waits the given seconds. Defaults to `time.sleep`.
        clock: Reads the time in seconds. Defaults to `time.monotonic`.
        chunk_size: The most rows one request carries.
        max_requests: The most requests the run sends.
        max_seconds: The longest the run may take.

    Returns:
        The findings, the rows the route refused, the graph generations, and why
        the run stopped early if it did. A run stops at the first request that
        fails on the network, waits more than `MAX_WAIT_SECONDS` on 503 and 429
        answers, meets a 502 or 504 past `GATEWAY_RETRIES`, is refused other than
        for one of its rows, is redirected, gets an answer it cannot read, or
        would pass either budget.
    """
    if post is None:
        with _httpx_post() as client_post:
            return check_rows(
                rows,
                url=url,
                post=client_post,
                sleep=sleep,
                clock=clock,
                chunk_size=chunk_size,
                max_requests=max_requests,
                max_seconds=max_seconds,
            )

    result = CheckResult()
    sender = _Sender(
        url=url,
        post=post,
        sleep=sleep,
        clock=clock,
        max_requests=max_requests,
        max_seconds=max_seconds,
        requests_left=max_requests,
        deadline=clock() + max_seconds,
    )
    # The index in `rows` of every row not yet answered or refused, in order.
    pending = list(range(len(rows)))
    try:
        while pending:
            sent = pending[:chunk_size]
            response = sender.send([rows[index] for index in sent])
            if response.status_code == 400:
                refused = _refused_row(response, len(sent))
                if refused is None:
                    raise GraphCheckError(
                        StopReason.REFUSED, f"answer 400, {_detail(response)}"
                    )
                result.failed_rows.append(
                    FailedRow(row=sent[refused], detail=_detail(response))
                )
                del pending[refused]
                continue
            del pending[: _keep_answer(response, sent, result)]
    except GraphCheckError as error:
        result.stopped = error
    return result


@dataclass(kw_only=True)
class _Sender:
    """Posts chunks within the run's request and time budgets"""

    url: str
    post: Post
    sleep: Callable[[float], None]
    clock: Callable[[], float]
    max_requests: int
    max_seconds: float
    requests_left: int
    deadline: float

    def send(self, rows: list[CheckRow]) -> httpx.Response:
        """Post one chunk until it answers 200 or 400"""
        waited = 0.0
        gateway_failures = 0
        while True:
            response = self._post_once(rows)
            status = response.status_code
            if status in (200, 400):
                return response
            problem = _body(response)
            if problem.get("retryable") is False:
                raise GraphCheckError(
                    StopReason.NOT_RETRYABLE,
                    f"answer {status}, {_problem_text(response)}",
                )
            if status in (429, 503):
                wait = _wait_seconds(response)
                if waited + wait > MAX_WAIT_SECONDS:
                    raise GraphCheckError(
                        StopReason.BUSY,
                        f"still answering {status} after {waited:g} seconds",
                        message=BUSY_CAUSES.get(_cause_code(response)),
                    )
                self._sleep(wait)
                waited += wait
                continue
            if 300 <= status < 400:
                location = response.headers.get("Location", "no location given")
                raise GraphCheckError(
                    StopReason.REDIRECTED, f"answer {status} to {location}"
                )
            if status in (502, 504) and gateway_failures < GATEWAY_RETRIES:
                gateway_failures += 1
                self._sleep(GATEWAY_RETRY_SECONDS)
                continue
            reason = StopReason.FAILED if status >= 500 else StopReason.REFUSED
            raise GraphCheckError(reason, f"answer {status}, {_problem_text(response)}")

    def _post_once(self, rows: list[CheckRow]) -> httpx.Response:
        """One request, if both budgets allow it, within the time left"""
        if self.requests_left <= 0:
            raise self._budget_spent(StopReason.REQUEST_BUDGET, "no requests left")
        time_left = self.deadline - self.clock()
        if time_left <= 0:
            raise self._budget_spent(StopReason.TIME_BUDGET, "no time left")
        self.requests_left -= 1
        timeout = min(REQUEST_TIMEOUT_SECONDS, time_left)
        try:
            return self.post(self.url, {"rows": rows}, timeout)
        except httpx.HTTPError as error:
            # A timeout cut short by the time left, or one that ends past the
            # deadline, means the run reached its limit. Any other error is an
            # outage.
            out_of_time = isinstance(error, httpx.TimeoutException) and (
                timeout < REQUEST_TIMEOUT_SECONDS or self.clock() >= self.deadline
            )
            detail = type(error).__name__
            if out_of_time:
                raise self._budget_spent(StopReason.TIME_BUDGET, detail) from error
            raise GraphCheckError(StopReason.UNREACHABLE, detail) from error

    def _sleep(self, seconds: float) -> None:
        """Wait, unless the wait would end past the deadline"""
        if self.clock() + seconds >= self.deadline:
            raise self._budget_spent(
                StopReason.TIME_BUDGET, f"a {seconds:g}-second wait would pass it"
            )
        self.sleep(seconds)

    def _budget_spent(self, reason: StopReason, detail: str) -> GraphCheckError:
        """The error for a spent budget, naming the budget this run was given"""
        if reason is StopReason.TIME_BUDGET:
            message = f"the check reached its {self.max_seconds / 60:g}-minute limit"
        else:
            message = f"the check reached its limit of {self.max_requests:,} requests"
        return GraphCheckError(reason, detail, message=message)


def _keep_answer(response: httpx.Response, sent: list[int], result: CheckResult) -> int:
    """Add a 200 answer to `result` once every part of it is as the route documents.

    `sent` holds the index in the run's rows of each row the request carried.
    Each finding's `row` is turned from an index in the request into one in the
    run's rows.

    Returns:
        The number of rows the route checked, from the first sent.
    """
    try:
        answer = response.json()
    except ValueError:
        answer = None
    if not isinstance(answer, dict):
        raise _unexpected("a body that is not a JSON object")
    generation = answer.get("generation")
    if "generation" not in answer or not isinstance(generation, str | None):
        raise _unexpected("no generation")
    rows_checked = answer.get("rows_checked")
    if not _is_index(rows_checked):
        raise _unexpected("no count of rows checked")
    if not 1 <= rows_checked <= len(sent):
        raise _unexpected(f"a count of {rows_checked:,} checked for {len(sent):,} sent")
    findings = answer.get("findings")
    if not isinstance(findings, list):
        raise _unexpected("no list of findings")
    if not all(_is_finding(finding, rows_checked) for finding in findings):
        raise _unexpected("a finding that does not match the rows checked")

    if generation not in result.generations:
        result.generations.append(generation)
    result.findings.extend(
        finding | {"row": sent[finding["row"]]} for finding in findings
    )
    result.checked_rows.extend(sent[:rows_checked])
    return rows_checked


def _is_index(value: Any) -> bool:
    """Whether `value` is a JSON integer of zero or more"""
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _is_finding(finding: Any, rows_checked: int) -> bool:
    """Whether `finding` has the members the route documents, about a row checked"""
    return (
        isinstance(finding, dict)
        and _is_index(finding.get("row"))
        and finding["row"] < rows_checked
        and isinstance(finding.get("field"), str)
        and isinstance(finding.get("property"), str | None)
        and "property" in finding
        and finding.get("subject") in SUBJECTS
        and "value" in finding
        and "served" in finding
        and isinstance(finding.get("code"), str)
        and finding.get("severity") in SEVERITIES
    )


def _refused_row(response: httpx.Response, sent: int) -> int | None:
    """The index in the request of the row a 400 answer refuses, if it names one"""
    pointer = _body(response).get("pointer")
    if not isinstance(pointer, str):
        return None
    match = _ROW_POINTER.match(pointer)
    if match is None or int(match[1]) >= sent:
        return None
    return int(match[1])


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


def _cause_code(response: httpx.Response) -> str | None:
    """The `cause.code` of a problem document, if it has one"""
    cause = _body(response).get("cause")
    code = cause.get("code") if isinstance(cause, dict) else None
    return code if isinstance(code, str) else None


def _problem_text(response: httpx.Response) -> str:
    """The problem's code and detail, or what of them the answer gives"""
    code = _body(response).get("code")
    detail = _detail(response)
    return f"{code}: {detail}" if isinstance(code, str) else detail


def _detail(response: httpx.Response) -> str:
    """The reason a problem document gives, or a note that there is none"""
    body = _body(response)
    detail = body.get("detail")
    if isinstance(body.get("code"), str) and isinstance(detail, str) and detail:
        return detail
    return "no detail given"


def _wait_seconds(response: httpx.Response) -> float:
    """The seconds a 503 or 429 answer's `Retry-After` asks for.

    The wait is at least `MIN_WAIT_SECONDS`, and `DEFAULT_WAIT_SECONDS` when the
    answer names no wait in seconds.
    """
    try:
        seconds = float(response.headers.get("Retry-After", ""))
    except ValueError:
        return DEFAULT_WAIT_SECONDS
    # Also false for NaN.
    if not seconds >= 0:
        return DEFAULT_WAIT_SECONDS
    return max(seconds, MIN_WAIT_SECONDS)
