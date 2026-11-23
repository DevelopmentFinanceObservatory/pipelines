from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from unittest.mock import patch

import httpx
import pytest

from dfo_pipelines.extraction.wikibase import LatestRevision, fetch_latest_revision

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


def mock_http(handler: Callable[[httpx.Request], httpx.Response]):
    """Patch the module's httpx.Client so requests are served by handler"""
    real_client = httpx.Client

    def client(**kwargs: Any) -> httpx.Client:
        return real_client(**kwargs, transport=httpx.MockTransport(handler))

    return patch("dfo_pipelines.extraction.wikibase.httpx.Client", side_effect=client)


def api(
    *, revisions: list[dict] = REVISIONS
) -> Callable[[httpx.Request], httpx.Response]:
    """An API answering the revision query, as Wikibase does"""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["list"] == "allrevisions":
            return httpx.Response(200, json={"query": {"allrevisions": revisions}})
        return httpx.Response(400)

    return handler


def test_reads_the_latest_revision() -> None:
    """Test the lookup returns the id and timestamp of the latest revision"""
    with mock_http(api()):
        revision = fetch_latest_revision(api_url=API_URL, namespaces=(120, 122))

    assert revision == LatestRevision(
        revision_id=2982, revised_at=datetime(2026, 11, 10, 13, 8, 34, tzinfo=UTC)
    )


def test_reads_revisions_in_the_given_namespaces_only() -> None:
    """Test the revision query is limited to the namespaces asked for"""
    namespaces: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        namespaces.append(request.url.params["arvnamespace"])
        return api()(request)

    with mock_http(handler):
        fetch_latest_revision(api_url=API_URL, namespaces=(120, 122))

    assert namespaces == ["120|122"]


def test_raises_when_the_namespaces_have_no_revisions() -> None:
    """Test namespaces with no revisions fail, since they leave nothing to compile"""
    with mock_http(api(revisions=[])), pytest.raises(RuntimeError, match="revisions"):
        fetch_latest_revision(api_url=API_URL, namespaces=(120, 122))


def test_raises_on_an_api_error() -> None:
    """Test an error payload fails the lookup, though the API answers it with a 200"""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"error": {"code": "badvalue", "info": "Unrecognized value"}}
        )

    with mock_http(handler), pytest.raises(RuntimeError, match="badvalue"):
        fetch_latest_revision(api_url=API_URL, namespaces=(120, 122))


def test_raises_on_an_http_error() -> None:
    """Test a failed request fails the lookup"""
    with (
        mock_http(lambda request: httpx.Response(503)),
        pytest.raises(httpx.HTTPStatusError),
    ):
        fetch_latest_revision(api_url=API_URL, namespaces=(120, 122))
