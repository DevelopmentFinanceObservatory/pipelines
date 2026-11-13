from collections.abc import Iterator

import duckdb
import pytest
from duckdb import DuckDBPyConnection

from tests.graph_check.tables import STATVARS_SQL


@pytest.fixture
def connection() -> Iterator[DuckDBPyConnection]:
    """An in-memory database holding `SPEC`'s StatVar table"""
    with duckdb.connect() as connection:
        connection.execute("create schema checked")
        connection.execute(STATVARS_SQL)
        yield connection
