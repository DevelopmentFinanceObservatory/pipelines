"""Read and write tables in the DuckDB warehouse"""

from collections.abc import Mapping
from typing import Any

import pyarrow.parquet as pq
from duckdb import DuckDBPyConnection

from dfo_pipelines.extraction.metadata import ExtractionMetadata


def load_parquet(
    connection: DuckDBPyConnection,
    *,
    path: str,
    schema: str,
    table: str,
    renames: Mapping[str, str] | None = None,
) -> int:
    """Load the parquet file at `path` into `schema`.`table`.

    The table is replaced rather than appended to.

    Args:
        connection: Connection to the database the table is written to.
        path: Location of the parquet file.
        schema: Schema to write into, created if it does not exist.
        table: Table to write, replaced if it does.
        renames: New name for each column to rename, keyed by its name in the
            file. Columns are aliased by position, so two columns whose names
            differ only in case can be given distinct names.

    Returns:
        int: Count of rows written.
    """
    select_statement = "SELECT * FROM read_parquet(?)"
    if renames:
        published = pq.read_schema(path).names
        aliases = [renames.get(name, name) for name in published]
        columns = ", ".join('"' + alias.replace('"', '""') + '"' for alias in aliases)
        select_statement += f" AS source({columns})"
    connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    connection.execute(
        f"CREATE OR REPLACE TABLE {schema}.{table} AS {select_statement}", [path]
    )
    return connection.sql(f"SELECT COUNT(*) FROM {schema}.{table}").fetchall()[0][0]


def load_extraction_metadata(
    connection: DuckDBPyConnection,
    *,
    metadata: ExtractionMetadata,
    schema: str,
    table: str,
) -> int:
    """Load `metadata` into `schema`.`table`.

    The table is replaced rather than appended to, showing the download
    date for the data that is currently loaded in the raw table.

    Args:
        connection: Connection to the database the table is written to.
        metadata: The extraction metadata, its release dates written as
            `<name>_release_date` columns in their order.
        schema: Schema to write into, created if it does not exist.
        table: Table to write, replaced if it does.

    Returns:
        int: Count of rows written.
    """
    connection.execute(f"create schema if not exists {schema}")
    release_columns = ", ".join(
        f"cast(? as date) as {name}_release_date" for name in metadata.release_dates
    )
    connection.execute(
        f"create or replace table {schema}.{table} as "
        f"select cast(? as timestamptz) as downloaded_at, {release_columns}",
        [metadata.downloaded_at, *metadata.release_dates.values()],
    )
    return connection.sql(f"select count(*) from {schema}.{table}").fetchall()[0][0]


def get_model_rows(connection: DuckDBPyConnection, model: str) -> list[dict[str, Any]]:
    """Retrieves every row of a model as a dict, keyed on column name"""
    return connection.sql(f"select * from {model}").to_arrow_table().to_pylist()
