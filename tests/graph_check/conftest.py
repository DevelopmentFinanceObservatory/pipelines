from collections.abc import Iterator

import duckdb
import pytest
from duckdb import DuckDBPyConnection

from tests.graph_check.tables import STATVARS_SQL


@pytest.fixture(autouse=True)
def _no_ci_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the CI job's step summary and commit out of every test"""
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    monkeypatch.delenv("GRAPH_CHECK_COMMIT", raising=False)


@pytest.fixture
def connection() -> Iterator[DuckDBPyConnection]:
    """An in-memory database holding `SPEC`'s StatVar table"""
    with duckdb.connect() as connection:
        connection.execute("create schema checked")
        connection.execute(STATVARS_SQL)
        yield connection
