from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from typing import Self

import dagster as dg
import numpy as np
import pandas as pd
from dagster_duckdb import DuckDBResource

from dfo_pipelines.defs.resources import UPathFileStoreResource


def meta[T](result: dg.MaterializeResult, key: str, kind: type[T]) -> T:
    assert result.metadata is not None
    value = result.metadata[key]
    assert isinstance(value, kind)
    return value


def row_count(duckdb: DuckDBResource, table: str) -> int:
    """Number of rows in a database table"""
    with duckdb.get_connection() as connection:
        return connection.sql(f"select count(*) from {table}").fetchall()[0][0]


def column_list(duckdb: DuckDBResource, table: str) -> list:
    """List columns in a database table"""
    with duckdb.get_connection() as connection:
        return connection.sql(f"select * from {table}").columns


def list_first_row(duckdb: DuckDBResource, table: str) -> list:
    """List first row in a database table"""
    with duckdb.get_connection() as connection:
        return (
            connection.sql(f"select * from {table} limit 1")
            .df()
            .values.flatten()
            .tolist()
        )


def list_parquet_columns(parquet_file: Path) -> list:
    """List columns in a parquet file"""
    return list(pd.read_parquet(parquet_file).columns.values)


def list_parquet_first_row(parquet_file: Path) -> list:
    """List first row in a parquet file"""
    return [
        x.item() if hasattr(x, "item") else x
        for x in pd.read_parquet(parquet_file).iloc[0].replace({np.nan: None})
    ]


class AutomationHarness:
    """Drives a source and its raw assets against an ephemeral instance, as the daemon does.

    Automation conditions are evaluated with a single cursor advancing over the
    raw assets, one tick at a time.

    Subclasses patch their pipeline's network layer for as long as the context
    is open, from the `_start_mocks` hook.
    """

    def __init__(
        self,
        tmp_path: Path,
        *,
        source: dg.SourceAsset,
        raw_assets: list[dg.AssetsDefinition],
    ) -> None:
        self._source = source
        self._raw_assets = raw_assets
        self._instance = dg.DagsterInstance.ephemeral()
        self._resources = {
            "raw_file_store": UPathFileStoreResource(base_path=str(tmp_path / "raw")),
            "duckdb": DuckDBResource(database=str(tmp_path / "database.duckdb")),
        }
        self._defs = dg.Definitions(
            assets=[source, *raw_assets],
            jobs=[
                dg.define_asset_job(
                    f"observe_{source.key.to_python_identifier()}",
                    selection=dg.AssetSelection.assets(source.key),
                )
            ],
            resources=self._resources,
        )
        self._last_tick: dg.EvaluateAutomationConditionsResult | None = None
        self._stack = ExitStack()

    def __enter__(self) -> Self:
        self._stack.enter_context(self._instance)
        try:
            self._start_mocks()
        except BaseException:
            self._stack.close()
            raise
        return self

    def _start_mocks(self) -> None:
        """Patch the pipeline's network layer, entered for the context's lifetime"""
        raise NotImplementedError

    def __exit__(self, *exc_info: object) -> None:
        self._stack.close()

    def observe(self) -> dg.ExecuteInProcessResult:
        """Observe the source at the current version

        Observations run through an explicitly resolved job because
        dg.materialize() accepts a source asset but never calls its observe
        function.
        """
        result = self._defs.resolve_job_def(
            f"observe_{self._source.key.to_python_identifier()}"
        ).execute_in_process(instance=self._instance)
        assert result.success
        return result

    def materialize(self, *assets: dg.AssetsDefinition) -> None:
        """Materialize assets, as a run launched by the daemon would"""
        result = dg.materialize(
            list(assets), resources=self._resources, instance=self._instance
        )
        assert result.success

    def source_tick(self, at: datetime) -> int:
        """Evaluate the source's cron condition at a fixed time, advancing the cursor

        The caller supplies the time because on_cron fires relative to the
        evaluation clock, which defaults to the real wall clock. A test run
        lasts seconds: the first tick is an initial evaluation, and no midnight
        UTC boundary ever falls between two ticks, so the condition could never
        fire — the result would be 0 on every tick even if the condition were
        deleted. A fixed evaluation_time past midnight makes the boundary fall
        between ticks deterministically.
        """
        result = dg.evaluate_automation_conditions(
            defs=self._defs,
            instance=self._instance,
            asset_selection=dg.AssetSelection.assets(self._source.key),
            cursor=self._last_tick.cursor if self._last_tick else None,
            evaluation_time=at,
        )
        self._last_tick = result
        return result.get_num_requested(self._source.key)

    def tick(self) -> dict[str, int]:
        """Evaluate the raw assets' automation conditions, advancing the cursor

        The source is left out of the selection because its observations are
        driven explicitly by observe() rather than by its daily condition.
        """
        result = dg.evaluate_automation_conditions(
            defs=self._defs,
            instance=self._instance,
            asset_selection=dg.AssetSelection.assets(*self._raw_assets),
            cursor=self._last_tick.cursor if self._last_tick else None,
        )
        self._last_tick = result
        return {
            asset.key.to_user_string(): result.get_num_requested(asset.key)
            for asset in self._raw_assets
        }
