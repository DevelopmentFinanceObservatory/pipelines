from collections.abc import Mapping
from contextlib import ExitStack
from datetime import UTC, date, datetime
from functools import cache
from pathlib import Path
from types import MappingProxyType
from typing import Self

import dagster as dg
import pyarrow as pa
import pyarrow.parquet as pq
from dagster_duckdb import DuckDBResource
from upath import UPath

from dfo_pipelines.definitions import defs
from dfo_pipelines.extraction.metadata import write_extraction_metadata
from dfo_pipelines.orchestration.resources import UPathFileStoreResource


@cache
def project_definitions() -> dg.Definitions:
    """The project's definitions, loaded as dg dev and dg check defs load them"""
    return defs()


def meta[T](result: dg.MaterializeResult, key: str, kind: type[T]) -> T:
    assert result.metadata is not None
    value = result.metadata[key]
    assert isinstance(value, kind)
    return value


def write_landing_directory(
    directory: Path,
    *,
    tables: Mapping[str, pa.Table],
    downloaded_at: datetime = datetime(2026, 8, 3, 12, tzinfo=UTC),
    release_dates: Mapping[str, date] = MappingProxyType({"source": date(2026, 7, 7)}),
) -> Path:
    """A landing directory holding each of `tables` as a parquet file under its stem,
    with the extraction metadata"""
    directory.mkdir(parents=True)
    for stem, table in tables.items():
        pq.write_table(table, directory / f"{stem}.parquet")
    write_extraction_metadata(
        UPath(directory), downloaded_at=downloaded_at, release_dates=release_dates
    )
    return directory


def load_raw_tables(
    assets: dg.AssetsDefinition,
    *,
    duckdb: DuckDBResource,
    directory: Path,
    raise_on_error: bool = True,
) -> dg.ExecuteInProcessResult:
    """Run the raw tables in a run whose landing asset returned `directory`"""
    [landing_key] = assets.keys_by_input_name.values()

    @dg.asset(key=landing_key)
    def landing() -> str:
        return str(directory)

    return dg.materialize(
        [landing, assets], resources={"duckdb": duckdb}, raise_on_error=raise_on_error
    )


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


def logged_messages(
    assets: list[dg.AssetsDefinition], resources: dict[str, object]
) -> list[str]:
    """The run log's messages from materializing the assets, other than event records"""
    instance = dg.DagsterInstance.ephemeral()
    result = dg.materialize(assets, resources=resources, instance=instance)
    assert result.success
    return [
        entry.user_message
        for entry in instance.all_logs(result.run_id)
        if entry.dagster_event is None
    ]


class AutomationHarness:
    """Drives a source and its raw assets against an ephemeral instance, as the daemon does.

    Automation conditions are evaluated with a single cursor advancing over the
    raw assets, one tick at a time.

    Subclasses patch their pipeline's network layer for as long as the context
    is open, from the `_start_mocks` hook, and pass any resource their assets
    take beyond the file store and the database.
    """

    def __init__(
        self,
        tmp_path: Path,
        *,
        source: dg.SourceAsset,
        raw_assets: list[dg.AssetsDefinition],
        resources: dict[str, object] | None = None,
    ) -> None:
        self._source = source
        self._raw_assets = raw_assets
        self._instance = dg.DagsterInstance.ephemeral()
        self._resources = {
            "landing_file_store": UPathFileStoreResource(
                base_path=str(tmp_path / "landing")
            ),
            "duckdb": DuckDBResource(database=str(tmp_path / "database.duckdb")),
            **(resources or {}),
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
            key.to_user_string(): result.get_num_requested(key)
            for asset in self._raw_assets
            for key in asset.keys
        }
