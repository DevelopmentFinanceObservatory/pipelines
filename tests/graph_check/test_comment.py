"""Tests of the graph check comment"""

from dataclasses import dataclass, field

import httpx
import pytest

from dfo_pipelines.graph_check.comment import (
    BOT_LOGIN,
    MARKER,
    Comment,
    GitHubPullRequest,
    comment_body,
    publish,
)
from tests.graph_check.tables import spans

HEAD = "0123456789abcdef"
BODY = f"{MARKER}\nNo problems found."


@dataclass
class FakePullRequest:
    """A pull request that records the calls `publish` makes"""

    head: str = HEAD
    existing: list[Comment] = field(default_factory=list)
    calls: list[tuple[str, ...]] = field(default_factory=list)

    def head_sha(self) -> str:
        return self.head

    def comments(self) -> list[Comment]:
        return self.existing

    def create_comment(self, body: str) -> None:
        self.calls.append(("create", body))

    def update_comment(self, comment_id: int, body: str) -> None:
        self.calls.append(("update", str(comment_id), body))

    def delete_comment(self, comment_id: int) -> None:
        self.calls.append(("delete", str(comment_id)))


def marked(comment_id: int, *, author: str = BOT_LOGIN) -> Comment:
    return Comment(id=comment_id, author=author, body=f"{MARKER}\nolder report")


@pytest.mark.parametrize(
    ("existing", "calls"),
    [
        ([], [("create", BODY)]),
        (
            [Comment(id=1, author="someone", body="looks good"), marked(2)],
            [("update", "2", BODY)],
        ),
        (
            [marked(2), marked(3, author="someone"), marked(4), marked(5)],
            [("update", "2", BODY), ("delete", "4"), ("delete", "5")],
        ),
        (
            [Comment(id=1, author=BOT_LOGIN, body=f"before\n{MARKER}")],
            [("create", BODY)],
        ),
    ],
    ids=["none", "one", "duplicates", "marker-not-first"],
)
def test_publish_keeps_the_first_marked_bot_comment(
    existing: list[Comment], calls: list[tuple[str, ...]]
) -> None:
    pull_request = FakePullRequest(existing=existing)

    publish(pull_request, body=BODY, commit=HEAD)

    assert pull_request.calls == calls


def test_publish_leaves_the_comment_to_the_run_for_a_newer_head() -> None:
    pull_request = FakePullRequest(head="fedcba9876543210", existing=[marked(2)])

    publish(pull_request, body=BODY, commit=HEAD)

    assert pull_request.calls == []


def test_the_comment_starts_with_the_marker_and_keeps_the_report() -> None:
    report = "No problems found.\n"

    assert comment_body(report, commit=HEAD) == f"{MARKER}\n{report}"


@pytest.mark.parametrize("report", [None, ""], ids=["missing", "empty"])
def test_a_missing_report_comments_that_the_check_did_not_run(
    report: str | None,
) -> None:
    lines = comment_body(report, commit=HEAD, base_branch="dev").split("\n")

    assert lines[0] == MARKER
    assert lines[1] == (
        "The graph check did not run: the check step wrote no report, and the job "
        "log says why."
    )
    assert spans(lines[3]) == ["0123456", "dev"]


def test_github_comments_are_read_across_every_page() -> None:
    pages = {
        "1": (
            [{"id": 1, "user": {"login": BOT_LOGIN}, "body": "a"}],
            '<https://api.test/repos/o/r/issues/7/comments?page=2>; rel="next"',
        ),
        "2": ([{"id": 2, "user": {"login": "someone"}, "body": "b"}], None),
    }

    def handler(request: httpx.Request) -> httpx.Response:
        items, link = pages[request.url.params.get("page", "1")]
        return httpx.Response(200, json=items, headers={"Link": link} if link else {})

    client = httpx.Client(
        base_url="https://api.test", transport=httpx.MockTransport(handler)
    )
    pull_request = GitHubPullRequest(client, repository="o/r", number=7)

    assert pull_request.comments() == [
        Comment(id=1, author=BOT_LOGIN, body="a"),
        Comment(id=2, author="someone", body="b"),
    ]
