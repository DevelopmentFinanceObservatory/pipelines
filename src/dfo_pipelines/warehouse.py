"""Read and write tables in the DuckDB warehouse"""

import re
from collections.abc import Collection, Mapping
from typing import Any

import pyarrow.parquet as pq
from duckdb import DuckDBPyConnection
from upath import UPath

from dfo_pipelines.extraction.metadata import read_extraction_metadata

_RELEASE_NAME_PATTERN = re.compile(r"[a-z][a-z0-9_]*")


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
    directory: UPath,
    schema: str,
    table: str,
    release_names: Collection[str],
) -> int:
    """Load the extraction metadata recorded in `directory` into `schema`.`table`.

    The table is replaced rather than appended to, showing the download
    date for the data that is currently loaded in the raw table.

    Args:
        connection: Connection to the database the table is written to.
        directory: Directory holding the files the extraction wrote.
        schema: Schema to write into, created if it does not exist.
        table: Table to write, replaced if it does.
        release_names: Names the recorded `<name>_release_date` keys must
            match. Also the order the `<name>_release_date` columns are
            written in.

    Returns:
        int: Count of rows written.

    Raises:
        FileNotFoundError: If the directory holds no record.
        ValueError: If a recorded release date key does not match
            `release_names`, or a name is not a valid identifier.
    """
    for name in release_names:
        if not _RELEASE_NAME_PATTERN.fullmatch(name):
            raise ValueError(
                f"Invalid release name {name!r}: must match "
                f"{_RELEASE_NAME_PATTERN.pattern!r}"
            )
    metadata = read_extraction_metadata(directory, release_names=release_names)
    connection.execute(f"create schema if not exists {schema}")
    release_columns = ", ".join(
        f"cast(? as date) as {name}_release_date" for name in release_names
    )
    connection.execute(
        f"create or replace table {schema}.{table} as "
        f"select cast(? as timestamptz) as downloaded_at, {release_columns}",
        [
            metadata.downloaded_at,
            *(metadata.release_dates[name] for name in release_names),
        ],
    )
    return connection.sql(f"select count(*) from {schema}.{table}").fetchall()[0][0]


def get_model_rows(connection: DuckDBPyConnection, model: str) -> list[dict[str, Any]]:
    """Retrieves every row of a model as a dict, keyed on column name, in the order
    of their values"""
    return (
        connection.sql(f"select * from {model} order by all")
        .to_arrow_table()
        .to_pylist()
    )
