"""Post a graph check report as the pull request's graph check comment.

Run it after `scripts/graph_check.py`, in the pull request workflow:

    uv run python -m dfo_pipelines.graph_check.comment report.md

The comment starts with `MARKER`, a hidden line that finds the comment an earlier
run wrote. `publish` updates the first comment `BOT_LOGIN` wrote with the marker
and deletes every later one, or posts a new comment when there is none. When the
pull request's head has moved past the commit the run checked, it leaves the
comment to the run for the newer head. When the check wrote no report, the comment
says the check did not run.

The script reads `GITHUB_TOKEN`, `GITHUB_REPOSITORY`, `GITHUB_API_URL`,
`PULL_REQUEST`, `GRAPH_CHECK_COMMIT`, the pull request head commit the run
checked, and `GRAPH_CHECK_BASE_BRANCH`, the branch it was merged into for the
build.
"""

import argparse
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import httpx

from dfo_pipelines.graph_check.report import CheckSummary, render_report

# The first line of every comment. Markdown renders it as nothing.
MARKER = "<!-- dfo-graph-check -->"

# The author of every comment the workflow writes with its own token.
BOT_LOGIN = "github-actions[bot]"

# Why the check did not run, when the check step wrote no report.
NO_REPORT = "the check step wrote no report, and the job log says why"


@dataclass(frozen=True, kw_only=True)
class Comment:
    """One comment on the pull request"""

    id: int
    author: str
    body: str


class PullRequest(Protocol):
    """The calls `publish` makes on one pull request"""

    def head_sha(self) -> str:
        """The pull request's head commit now"""
        ...

    def comments(self) -> list[Comment]:
        """Every comment on the pull request, oldest first"""
        ...

    def create_comment(self, body: str) -> None:
        """Post a new comment"""
        ...

    def update_comment(self, comment_id: int, body: str) -> None:
        """Replace a comment's body"""
        ...

    def delete_comment(self, comment_id: int) -> None:
        """Delete a comment"""
        ...


def comment_body(
    report: str | None, *, commit: str | None, base_branch: str | None = None
) -> str:
    """The comment for a report, or saying the check did not run when there is none

    Args:
        report: The report the check step wrote, or None when it wrote none.
        commit: The pull request head commit the run checked.
        base_branch: The branch the head was merged into for the build.
    """
    if not report:
        report = render_report(
            CheckSummary(not_run=NO_REPORT, commit=commit, base_branch=base_branch)
        )
    return f"{MARKER}\n{report}"


def publish(pull_request: PullRequest, *, body: str, commit: str) -> str:
    """Write `body` as the pull request's graph check comment and say what was done

    Args:
        pull_request: The pull request to comment on.
        body: The comment, starting with `MARKER`.
        commit: The pull request head commit the run checked.
    """
    head = pull_request.head_sha()
    if head != commit:
        return (
            f"The pull request's head is now {head}, so the run for that commit "
            "writes the comment."
        )
    marked = [
        comment.id
        for comment in pull_request.comments()
        if comment.author == BOT_LOGIN and comment.body.startswith(MARKER)
    ]
    if not marked:
        pull_request.create_comment(body)
        return "Posted the graph check comment."
    first, *later = marked
    pull_request.update_comment(first, body)
    for comment_id in later:
        pull_request.delete_comment(comment_id)
    deleted = f" Deleted the duplicates {', '.join(map(str, later))}." if later else ""
    return f"Updated comment {first}.{deleted}"


class GitHubPullRequest:
    """A pull request on GitHub, reached over the REST API"""

    def __init__(self, client: httpx.Client, *, repository: str, number: int) -> None:
        """
        Args:
            client: A client whose base URL is the API and which sends the token.
            repository: The repository as `owner/name`.
            number: The pull request number.
        """
        self._client = client
        self._repository = repository
        self._number = number

    def head_sha(self) -> str:
        """The pull request's head commit now"""
        response = self._client.get(f"/repos/{self._repository}/pulls/{self._number}")
        response.raise_for_status()
        return response.json()["head"]["sha"]

    def comments(self) -> list[Comment]:
        """Every comment on the pull request, oldest first, across every page"""
        comments = []
        url: str | None = (
            f"/repos/{self._repository}/issues/{self._number}/comments?per_page=100"
        )
        while url is not None:
            response = self._client.get(url)
            response.raise_for_status()
            comments.extend(
                Comment(id=item["id"], author=item["user"]["login"], body=item["body"])
                for item in response.json()
            )
            url = response.links.get("next", {}).get("url")
        return comments

    def create_comment(self, body: str) -> None:
        """Post a new comment"""
        self._client.post(
            f"/repos/{self._repository}/issues/{self._number}/comments",
            json={"body": body},
        ).raise_for_status()

    def update_comment(self, comment_id: int, body: str) -> None:
        """Replace a comment's body"""
        self._client.patch(
            f"/repos/{self._repository}/issues/comments/{comment_id}",
            json={"body": body},
        ).raise_for_status()

    def delete_comment(self, comment_id: int) -> None:
        """Delete a comment"""
        self._client.delete(
            f"/repos/{self._repository}/issues/comments/{comment_id}"
        ).raise_for_status()


def main(argv: Sequence[str] | None = None) -> int:
    """Post the report and return the exit status"""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("report", type=Path, help="the report the check wrote")
    args = parser.parse_args(argv)

    commit = os.environ["GRAPH_CHECK_COMMIT"]
    report = args.report.read_text() if args.report.is_file() else None
    body = comment_body(
        report, commit=commit, base_branch=os.environ.get("GRAPH_CHECK_BASE_BRANCH")
    )
    with httpx.Client(
        base_url=os.environ.get("GITHUB_API_URL", "https://api.github.com"),
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        timeout=30,
    ) as client:
        pull_request = GitHubPullRequest(
            client,
            repository=os.environ["GITHUB_REPOSITORY"],
            number=int(os.environ["PULL_REQUEST"]),
        )
        print(publish(pull_request, body=body, commit=commit))
    return 0


if __name__ == "__main__":
    sys.exit(main())
