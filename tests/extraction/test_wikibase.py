from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from unittest.mock import patch

import httpx
import pytest

from dfo_pipelines.extraction.wikibase import LatestEdit, fetch_latest_edit

API_URL = "https://wikibase.example/w/api.php"

REVISIONS = [
    {
        "pageid": 1341,
        "revisions": [
            {"revid": 2982, "parentid": 2390, "timestamp": "2026-11-10T13:08:34Z"}
        ],
        "ns": 120,
        "title": "Item:Q1323",
    }
]
DELETION = {
    "logid": 1700,
    "pageid": 0,
    "logpage": 916,
    "timestamp": "2026-10-08T14:08:33Z",
}


def mock_http(handler: Callable[[httpx.Request], httpx.Response]):
    """Patch the module's httpx.Client so requests are served by handler"""
    real_client = httpx.Client

    def client(**kwargs: Any) -> httpx.Client:
        return real_client(**kwargs, transport=httpx.MockTransport(handler))

    return patch("dfo_pipelines.extraction.wikibase.httpx.Client", side_effect=client)


def api(
    *,
    revisions: list[dict] = REVISIONS,
    deletions: dict[int, list[dict]] | None = None,
) -> Callable[[httpx.Request], httpx.Response]:
    """An API answering the revision and deletion log queries, as Wikibase does

    `deletions` are keyed on namespace, by default one deletion in namespace 120.
    The deletion log answers with its latest entry in the namespace asked for, or
    in any namespace when none is.
    """
    deletions_by_namespace = {120: [DELETION]} if deletions is None else deletions

    def handler(request: httpx.Request) -> httpx.Response:
        params = request.url.params
        match params["list"]:
            case "allrevisions":
                return httpx.Response(200, json={"query": {"allrevisions": revisions}})
            case "logevents" if params["letype"] == "delete":
                entries = (
                    deletions_by_namespace.get(int(params["lenamespace"]), [])
                    if "lenamespace" in params
                    else [
                        entry
                        for entries in deletions_by_namespace.values()
                        for entry in entries
                    ]
                )
                latest = sorted(entries, key=lambda entry: entry["logid"])[-1:]
                return httpx.Response(200, json={"query": {"logevents": latest}})
        return httpx.Response(400)

    return handler


def test_reads_the_latest_revision_and_deletion() -> None:
    """Test the lookup returns the ids and timestamps of the revision and deletion"""
    with mock_http(api()):
        edit = fetch_latest_edit(api_url=API_URL, namespaces=(120, 122))

    assert edit == LatestEdit(
        revision_id=2982,
        revised_at=datetime(2026, 11, 10, 13, 8, 34, tzinfo=UTC),
        deletion_log_id=1700,
        deleted_at=datetime(2026, 10, 8, 14, 8, 33, tzinfo=UTC),
    )


def test_reads_the_latest_deletion_across_the_namespaces() -> None:
    """Test the deletion is the latest of those in each namespace"""
    later = DELETION | {"logid": 1800, "timestamp": "2026-10-09T09:00:00Z"}

    with mock_http(api(deletions={120: [DELETION], 122: [later]})):
        edit = fetch_latest_edit(api_url=API_URL, namespaces=(120, 122))

    assert edit.deletion_log_id == 1800
    assert edit.deleted_at == datetime(2026, 10, 9, 9, tzinfo=UTC)


def test_ignores_a_deletion_outside_the_namespaces() -> None:
    """Test deleting a page the namespaces do not hold leaves the edit unchanged"""
    outside = DELETION | {"logid": 1800, "timestamp": "2026-10-09T09:00:00Z"}

    with mock_http(api(deletions={120: [DELETION], 0: [outside]})):
        edit = fetch_latest_edit(api_url=API_URL, namespaces=(120, 122))

    assert edit.deletion_log_id == 1700


def test_reads_revisions_in_the_given_namespaces_only() -> None:
    """Test the revision query is limited to the namespaces asked for"""
    namespaces: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["list"] == "allrevisions":
            namespaces.append(request.url.params["arvnamespace"])
        return api()(request)

    with mock_http(handler):
        fetch_latest_edit(api_url=API_URL, namespaces=(120, 122))

    assert namespaces == ["120|122"]


def test_an_instance_with_no_deletions_has_no_deletion_id() -> None:
    """Test an empty deletion log reads as no deletion, not as a failure"""
    with mock_http(api(deletions={})):
        edit = fetch_latest_edit(api_url=API_URL, namespaces=(120, 122))

    assert edit.deletion_log_id is None
    assert edit.deleted_at is None


def test_raises_when_the_namespaces_have_no_revisions() -> None:
    """Test namespaces with no revisions fail, since they leave nothing to compile"""
    with mock_http(api(revisions=[])), pytest.raises(RuntimeError, match="revisions"):
        fetch_latest_edit(api_url=API_URL, namespaces=(120, 122))


def test_raises_on_an_api_error() -> None:
    """Test an error payload fails the lookup, though the API answers it with a 200"""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"error": {"code": "badvalue", "info": "Unrecognized value"}}
        )

    with mock_http(handler), pytest.raises(RuntimeError, match="badvalue"):
        fetch_latest_edit(api_url=API_URL, namespaces=(120, 122))


def test_raises_on_an_http_error() -> None:
    """Test a failed request fails the lookup"""
    with (
        mock_http(lambda request: httpx.Response(503)),
        pytest.raises(httpx.HTTPStatusError),
    ):
        fetch_latest_edit(api_url=API_URL, namespaces=(120, 122))
