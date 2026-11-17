"""Load a parquet file into a DuckDB table"""

from collections.abc import Mapping

import pyarrow.parquet as pq
from duckdb import DuckDBPyConnection


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
