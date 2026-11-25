from abc import ABC, abstractmethod
from collections.abc import Mapping
from contextlib import AbstractContextManager
from typing import Any, override

import pyarrow.parquet as pq
from dagster_duckdb import DuckDBResource
from duckdb import DuckDBPyConnection
from upath import UPath

from dfo_pipelines.extraction.metadata import ExtractionMetadata
from dfo_pipelines.resources.base import StrictConfigurableResource


class WarehouseResource(StrictConfigurableResource, ABC):
    """Database the raw files are loaded into and the dbt models are read from

    Loads replace `schema`.`table`, creating the schema if needed, and return the
    table's row count. Reads name a model as `<schema>.<name>`.
    """

    @abstractmethod
    def load_parquet(
        self,
        *,
        path: str,
        schema: str,
        table: str,
        renames: Mapping[str, str] | None = None,
    ) -> int:
        """Load the parquet file at `path`

        `renames` gives the new name of each column to rename, keyed by its name in
        the file, aliased by position so names differing only in case stay apart.
        """

    @abstractmethod
    def load_extraction_metadata(
        self, *, metadata: ExtractionMetadata, schema: str, table: str
    ) -> int:
        """Load one row holding `downloaded_at`, then a `<name>_release_date` column
        for each of `metadata`'s release dates, in their order
        """

    @abstractmethod
    def load_csv_as_strings(self, *, path: str, schema: str, table: str) -> int:
        """Load the CSV file at `path`, read with a header row and every column as a
        string
        """

    @abstractmethod
    def get_rows(self, model: str) -> list[dict[str, Any]]:
        """Every row of `model` as a dict, keyed on column name"""

    @abstractmethod
    def write_table(self, model: str, *, path: str) -> int:
        """Write every row of `model` to `path` as a CSV file with a header, sorted
        ascending on every column in table order, nulls last, returning the count
        of rows written
        """

    @abstractmethod
    def write_partitions(
        self, model: str, *, column: str, directory: str, file_stem: str
    ) -> int:
        """Write the rows of `model` to `directory` as CSV files with a header, one
        or more for each value of `column`, returning the count of rows written

        Each file is named `<file_stem>_<value>`, then optionally a shard suffix,
        then `.csv`. Rows are sorted as `write_table` sorts them.
        """


class DuckDBWarehouseResource(WarehouseResource):
    """Warehouse in a DuckDB database"""

    database: str

    @override
    def load_parquet(
        self,
        *,
        path: str,
        schema: str,
        table: str,
        renames: Mapping[str, str] | None = None,
    ) -> int:
        select_statement = "select * from read_parquet(?)"
        if renames:
            published = pq.read_schema(path).names
            aliases = [renames.get(name, name) for name in published]
            columns = ", ".join(
                '"' + alias.replace('"', '""') + '"' for alias in aliases
            )
            select_statement += f" as source({columns})"
        return self._replace_table(schema, table, select_statement, [path])

    @override
    def load_extraction_metadata(
        self, *, metadata: ExtractionMetadata, schema: str, table: str
    ) -> int:
        release_columns = ", ".join(
            f"cast(? as date) as {name}_release_date" for name in metadata.release_dates
        )
        return self._replace_table(
            schema,
            table,
            f"select cast(? as timestamptz) as downloaded_at, {release_columns}",
            [metadata.downloaded_at, *metadata.release_dates.values()],
        )

    @override
    def load_csv_as_strings(self, *, path: str, schema: str, table: str) -> int:
        return self._replace_table(
            schema,
            table,
            "select * from read_csv(?, header = true, all_varchar = true)",
            [path],
        )

    @override
    def get_rows(self, model: str) -> list[dict[str, Any]]:
        with self._connection() as connection:
            return connection.sql(f"select * from {model}").to_arrow_table().to_pylist()

    @override
    def write_table(self, model: str, *, path: str) -> int:
        with self._connection() as connection:
            [(row_count,)] = connection.execute(
                f"copy (select * from {model} order by all) to ? (format csv, header)",
                [path],
            ).fetchall()
        return row_count

    @override
    def write_partitions(
        self, model: str, *, column: str, directory: str, file_stem: str
    ) -> int:
        with self._connection() as connection:
            partitions = connection.sql(
                f"select {column}, count(*) from {model}"
                f" group by {column} order by {column}"
            ).fetchall()
            for value, _ in partitions:
                connection.sql(
                    f"select * from {model} where {column} = ? order by all",
                    params=[value],
                ).write_csv(
                    str(UPath(directory) / f"{file_stem}_{value}.csv"), header=True
                )
        return sum(count for _, count in partitions)

    def _connection(self) -> AbstractContextManager[DuckDBPyConnection]:
        """Connection to the database, retried while another process holds its lock"""
        return DuckDBResource(database=self.database).get_connection()

    def _replace_table(
        self, schema: str, table: str, select_statement: str, parameters: list[Any]
    ) -> int:
        """Replace `schema`.`table` with the rows `select_statement` gives, returning
        its row count"""
        with self._connection() as connection:
            connection.execute(f"create schema if not exists {schema}")
            connection.execute(
                f"create or replace table {schema}.{table} as {select_statement}",
                parameters,
            )
            [(row_count,)] = connection.sql(
                f"select count(*) from {schema}.{table}"
            ).fetchall()
            return row_count
