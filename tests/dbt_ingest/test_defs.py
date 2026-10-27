import json
import subprocess
import sys
from pathlib import Path

import dagster as dg
import duckdb
import pytest

from dfo_pipelines.definitions import defs

FIXTURES_SQL = Path("scripts/load_ci_fixtures.sql")

# Prints the schema, name and row count of every table in the database.
TABLE_ROW_COUNTS = """
import json, sys
import duckdb

with duckdb.connect(sys.argv[1], read_only=True) as connection:
    tables = connection.sql(
        "select table_schema, table_name from information_schema.tables "
        "where table_type = 'BASE TABLE'"
    ).fetchall()
    print(json.dumps([
        [schema, name, connection.sql(f"select count(*) from {schema}.{name}").fetchone()[0]]
        for schema, name in tables
    ]))
"""


@pytest.fixture
def database(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Database holding the CI fixtures, as the project's DUCKDB_PATH"""
    database = tmp_path / "test.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(database))
    # Loads definitions as `dg dev` does, refreshing the dbt manifest, whose
    # relation names carry the database's catalog.
    monkeypatch.setenv("DAGSTER_IS_DEV_CLI", "1")
    with duckdb.connect(database) as connection:
        connection.execute(FIXTURES_SQL.read_text())
    return database


def test_table_models_report_their_row_count(database: Path) -> None:
    """Test every dbt table reports its row count on materialization and no view does"""
    definitions = defs()
    result = dg.materialize(
        [
            asset
            for asset in definitions.assets or []
            if isinstance(asset, dg.AssetsDefinition)
        ],
        selection=dg.AssetSelection.tag("dagster/kind/dbt", ""),
        resources=definitions.resources,
    )
    reported = {
        event.asset_key: event.materialization.metadata.get("dagster/row_count")
        for event in result.get_asset_materialization_events()
    }

    tables = {
        dg.AssetKey([schema, name]): count
        for schema, name, count in _table_row_counts(database)
    }

    assert {key: count.value for key, count in reported.items() if count} == {
        key: count for key, count in tables.items() if key in reported
    }


def _table_row_counts(database: Path) -> list[tuple[str, str, int]]:
    """Row count of every table, read in another process.

    dbt's in-process connection outlives the run and blocks a second
    connection with a different configuration from this one.
    """
    output = subprocess.run(
        [sys.executable, "-c", TABLE_ROW_COUNTS, str(database)],
        capture_output=True,
        check=True,
        text=True,
    ).stdout
    return json.loads(output)
