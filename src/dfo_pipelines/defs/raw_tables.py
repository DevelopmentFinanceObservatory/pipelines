from collections.abc import Collection, Mapping

import dagster as dg
import pyarrow.parquet as pq
from dagster_duckdb import DuckDBResource
from upath import UPath

from dfo_pipelines.extraction.metadata import load_extraction_metadata


def load_raw_table(
    duckdb: DuckDBResource,
    *,
    path: str,
    schema: str,
    table: str,
    renames: Mapping[str, str] | None = None,
    asset_key: dg.AssetKey | None = None,
) -> dg.MaterializeResult:
    """Load a raw parquet file into `schema`.`table`, aliasing columns by position when `renames` is given"""
    select_statement = "SELECT * FROM read_parquet(?)"
    if renames is not None:
        published = pq.read_schema(path).names
        aliases = [renames.get(name, name) for name in published]
        columns = ", ".join('"' + alias.replace('"', '""') + '"' for alias in aliases)
        select_statement += f" AS source({columns})"
    with duckdb.get_connection() as connection:
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        connection.execute(
            f"CREATE OR REPLACE TABLE {schema}.{table} AS {select_statement}", [path]
        )
        rows = connection.sql(f"SELECT COUNT(*) FROM {schema}.{table}").fetchall()[0][0]
    return dg.MaterializeResult(
        asset_key=asset_key,
        metadata={
            "dagster/table_name": dg.MetadataValue.text(f"{schema}.{table}"),
            "dagster/row_count": dg.MetadataValue.int(rows),
        },
        check_results=[
            dg.AssetCheckResult(
                asset_key=asset_key, check_name="table_not_empty", passed=(rows > 0)
            )
        ],
    )


def load_raw_metadata(
    duckdb: DuckDBResource,
    *,
    directory: UPath,
    schema: str,
    table: str,
    release_names: Collection[str],
) -> dg.MaterializeResult:
    """Load the extraction metadata in `directory` into `schema`.`table`"""
    with duckdb.get_connection() as connection:
        rows = load_extraction_metadata(
            connection,
            directory=directory,
            schema=schema,
            table=table,
            release_names=release_names,
        )
    return dg.MaterializeResult(
        metadata={
            "dagster/table_name": dg.MetadataValue.text(f"{schema}.{table}"),
            "dagster/row_count": dg.MetadataValue.int(rows),
        },
    )
