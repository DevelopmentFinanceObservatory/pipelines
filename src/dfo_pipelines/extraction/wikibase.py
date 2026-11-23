"""Read what a Wikibase instance last changed, through its MediaWiki API"""

from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime

import httpx

REQUEST_TIMEOUT_SECONDS = 30


@dataclass(frozen=True, kw_only=True)
class LatestRevision:
    """The latest revision of a page in a set of namespaces

    Attributes:
        revision_id: Id of the revision.
        revised_at: Instant the revision was saved.
    """

    revision_id: int
    revised_at: datetime


def fetch_latest_revision(
    *, api_url: str, namespaces: Collection[int]
) -> LatestRevision:
    """The latest revision in `namespaces` at `api_url`

    Args:
        api_url: The instance's `api.php` endpoint.
        namespaces: Namespaces whose revisions are read.

    Raises:
        RuntimeError: If the API returns an error or the namespaces have no
            revisions.
    """
    with httpx.Client(timeout=REQUEST_TIMEOUT_SECONDS) as client:
        response = client.get(
            api_url,
            params={
                "action": "query",
                "format": "json",
                "formatversion": "2",
                "list": "allrevisions",
                "arvnamespace": "|".join(str(namespace) for namespace in namespaces),
                "arvlimit": "1",
                "arvprop": "ids|timestamp",
            },
        )
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        raise RuntimeError(f"{api_url} returned an error: {payload['error']}")
    revisions = payload["query"]["allrevisions"]
    if not revisions:
        raise RuntimeError(f"{api_url} has no revisions in namespaces {namespaces}")
    [revision] = revisions[0]["revisions"]
    return LatestRevision(
        revision_id=revision["revid"],
        revised_at=datetime.fromisoformat(revision["timestamp"]),
    )
