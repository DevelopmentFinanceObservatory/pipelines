"""Read what a Wikibase instance last changed, through its MediaWiki API"""

from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime

import httpx

REQUEST_TIMEOUT_SECONDS = 30


@dataclass(frozen=True, kw_only=True)
class LatestEdit:
    """The latest revision and the latest deletion in a set of namespaces

    Attributes:
        revision_id: Id of the latest revision of a page in the namespaces.
        revised_at: Instant that revision was saved.
        deletion_log_id: Id of the latest deletion log entry in the namespaces,
            or None when nothing in them has been deleted.
        deleted_at: Instant of that deletion, or None.
    """

    revision_id: int
    revised_at: datetime
    deletion_log_id: int | None
    deleted_at: datetime | None


def _query(client: httpx.Client, api_url: str, params: dict[str, str]) -> dict:
    """The `query` member of an API response, raising on an API error"""
    response = client.get(
        api_url,
        params={"action": "query", "format": "json", "formatversion": "2", **params},
    )
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        raise RuntimeError(f"{api_url} returned an error: {payload['error']}")
    return payload["query"]


def fetch_latest_edit(*, api_url: str, namespaces: Collection[int]) -> LatestEdit:
    """The latest revision and the latest deletion in `namespaces` at `api_url`

    A deletion removes a page's revisions from the revision list, so the
    deletion log is read too: a deletion alone changes the result.

    Args:
        api_url: The instance's `api.php` endpoint.
        namespaces: Namespaces whose revisions are read.

    Raises:
        RuntimeError: If the API returns an error or the namespaces have no
            revisions.
    """
    with httpx.Client(timeout=REQUEST_TIMEOUT_SECONDS) as client:
        revisions = _query(
            client,
            api_url,
            {
                "list": "allrevisions",
                "arvnamespace": "|".join(str(namespace) for namespace in namespaces),
                "arvlimit": "1",
                "arvprop": "ids|timestamp",
            },
        )["allrevisions"]
        # The deletion log filters on one namespace at a time.
        deletions = [
            deletion
            for namespace in namespaces
            for deletion in _query(
                client,
                api_url,
                {
                    "list": "logevents",
                    "letype": "delete",
                    "lenamespace": str(namespace),
                    "lelimit": "1",
                    "leprop": "ids|timestamp",
                },
            )["logevents"]
        ]
    if not revisions:
        raise RuntimeError(f"{api_url} has no revisions in namespaces {namespaces}")
    [revision] = revisions[0]["revisions"]
    deletion = max(deletions, key=lambda entry: entry["logid"], default=None)
    return LatestEdit(
        revision_id=revision["revid"],
        revised_at=datetime.fromisoformat(revision["timestamp"]),
        deletion_log_id=deletion["logid"] if deletion else None,
        deleted_at=datetime.fromisoformat(deletion["timestamp"]) if deletion else None,
    )
