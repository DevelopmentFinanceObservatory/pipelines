from pathlib import Path

import dagster as dg
import pytest
from dagster_duckdb import DuckDBResource
from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.adapters.factory import reset_adapters

from dfo_pipelines.definitions import defs
from tests.helpers import row_count

FIXTURES_SQL = Path("scripts/load_ci_fixtures.sql")


@pytest.fixture
def duckdb(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DuckDBResource:
    """Database holding the CI fixtures, as the project's DUCKDB_PATH"""
    database = tmp_path / "test.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(database))
    # Loads definitions as `dg dev` does, refreshing the dbt manifest, whose
    # relation names carry the database's catalog.
    monkeypatch.setenv("DAGSTER_IS_DEV_CLI", "1")
    duckdb = DuckDBResource(database=str(database))
    with duckdb.get_connection() as connection:
        connection.execute(FIXTURES_SQL.read_text())
    return duckdb


def test_table_models_report_their_row_count(duckdb: DuckDBResource) -> None:
    """Test every dbt table reports its row count on materialization and no view does"""
    definitions = defs()
    assets = [
        asset
        for asset in definitions.assets or []
        if isinstance(asset, dg.AssetsDefinition)
    ]
    result = dg.materialize(
        assets,
        selection=dg.AssetSelection.tag("dagster/kind/dbt", ""),
        resources=definitions.resources,
    )
    # dagster-dbt counts through an adapter whose connection outlives the run and
    # blocks any connection with another configuration. Both calls are needed.
    DuckDBConnectionManager.close_all_connections()
    reset_adapters()

    reported = {
        event.asset_key: count.value
        for event in result.get_asset_materialization_events()
        if (count := event.materialization.metadata.get("dagster/row_count"))
    }
    tables = [
        spec.key
        for asset in assets
        for spec in asset.specs
        if spec.metadata.get("dagster-dbt/materialization_type") not in (None, "view")
    ]
    assert reported == {key: row_count(duckdb, ".".join(key.path)) for key in tables}
