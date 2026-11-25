import csv
import tempfile
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, override

import pyarrow as pa
import pyarrow.parquet as pq
from dagster_duckdb import DuckDBResource
from duckdb import DuckDBPyConnection
from google.cloud import bigquery
from upath import UPath

from dfo_pipelines.resources.base import StrictConfigurableResource


class WarehouseResource(StrictConfigurableResource, ABC):
    """Database the raw files are loaded into and the dbt models are read from

    Loads replace `schema`.`table` and return the table's row count. Reads name a
    model as `<schema>.<name>`.
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
    def load_rows(
        self, rows: Sequence[Mapping[str, Any]], *, schema: str, table: str
    ) -> int:
        """Load `rows`, with columns in the order of the first row's keys

        Column types follow the Python values: a timezone-aware `datetime` gives a
        timestamp with time zone and a `date` a date. A column holding only `None`
        has no type to take.
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
    def write_csv(
        self,
        model: str,
        *,
        directory: str,
        file_stem: str,
        partition_column: str | None = None,
    ) -> int:
        """Write the rows of `model` to `directory` as CSV files with a header,
        returning the count of rows written

        With `partition_column`, there is one or more files for each of its values,
        named `<file_stem>_<value>`; without it, `<file_stem>`. Either name may take
        a shard suffix before `.csv`. Rows are sorted ascending on every column in
        table order, nulls last.
        """


class DuckDBWarehouseResource(WarehouseResource):
    """Warehouse in a DuckDB database, whose loads create the schema if needed"""

    database: str

    def _connection(self) -> AbstractContextManager[DuckDBPyConnection]:
        """Connection to the database, retried while another process holds its lock"""
        return DuckDBResource(database=self.database).get_connection()

    @staticmethod
    def _replace_table(
        connection: DuckDBPyConnection,
        schema: str,
        table: str,
        select_statement: str,
        parameters: list[Any],
    ) -> int:
        """Replace `schema`.`table` with the rows `select_statement` gives, returning
        its row count"""
        connection.execute(f"create schema if not exists {schema}")
        connection.execute(
            f"create or replace table {schema}.{table} as {select_statement}",
            parameters,
        )
        [(row_count,)] = connection.sql(
            f"select count(*) from {schema}.{table}"
        ).fetchall()
        return row_count

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
        with self._connection() as connection:
            return self._replace_table(
                connection, schema, table, select_statement, [path]
            )

    @override
    def load_rows(
        self, rows: Sequence[Mapping[str, Any]], *, schema: str, table: str
    ) -> int:
        rows_to_load = pa.Table.from_pylist(list(rows))
        with self._connection() as connection:
            connection.register("rows_to_load", rows_to_load)
            return self._replace_table(
                connection, schema, table, "select * from rows_to_load", []
            )

    @override
    def load_csv_as_strings(self, *, path: str, schema: str, table: str) -> int:
        with self._connection() as connection:
            return self._replace_table(
                connection,
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
    def write_csv(
        self,
        model: str,
        *,
        directory: str,
        file_stem: str,
        partition_column: str | None = None,
    ) -> int:
        with self._connection() as connection:
            if partition_column is None:
                [(row_count,)] = connection.execute(
                    f"copy (select * from {model} order by all) to ?"
                    " (format csv, header)",
                    [str(UPath(directory) / f"{file_stem}.csv")],
                ).fetchall()
                return row_count
            partitions = connection.sql(
                f"select {partition_column}, count(*) from {model}"
                f" group by {partition_column} order by {partition_column}"
            ).fetchall()
            for value, _ in partitions:
                connection.sql(
                    f"select * from {model} where {partition_column} = ? order by all",
                    params=[value],
                ).write_csv(
                    str(UPath(directory) / f"{file_stem}_{value}.csv"), header=True
                )
        return sum(count for _, count in partitions)


class BigQueryWarehouseResource(WarehouseResource):
    """Warehouse in BigQuery, whose schemas are datasets that must already exist"""

    project: str
    location: str

    def _client(self) -> bigquery.Client:
        """Client for the project, using application default credentials"""
        return bigquery.Client(project=self.project, location=self.location)

    @staticmethod
    def _job_config(source_format: str, **options: Any) -> bigquery.LoadJobConfig:
        """Load job configuration replacing the destination table"""
        return bigquery.LoadJobConfig(
            source_format=source_format,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            **options,
        )

    @staticmethod
    def _row_count(job: bigquery.LoadJob) -> int:
        """Rows loaded by `job`, once it has finished"""
        job.result()
        # The API leaves out a count of zero
        return job.output_rows or 0

    def _load_file(self, path: Path, *, schema: str, table: str) -> int:
        """Load the local parquet file at `path`"""
        with self._client() as client, path.open("rb") as file:
            return self._row_count(
                client.load_table_from_file(
                    file,
                    f"{schema}.{table}",
                    job_config=self._job_config(bigquery.SourceFormat.PARQUET),
                )
            )

    @override
    def load_parquet(
        self,
        *,
        path: str,
        schema: str,
        table: str,
        renames: Mapping[str, str] | None = None,
    ) -> int:
        if renames:
            # BigQuery rejects a file whose column names differ only in case
            with (
                UPath(path).open("rb") as published,
                tempfile.TemporaryDirectory() as directory,
            ):
                source = pq.ParquetFile(published)
                renamed_schema = pa.schema(
                    field.with_name(renames.get(field.name, field.name))
                    for field in source.schema_arrow
                )
                renamed = Path(directory) / "renamed.parquet"
                with pq.ParquetWriter(renamed, renamed_schema) as writer:
                    for batch in source.iter_batches():
                        writer.write_batch(
                            pa.RecordBatch.from_arrays(
                                batch.columns, schema=renamed_schema
                            )
                        )
                return self._load_file(renamed, schema=schema, table=table)
        with self._client() as client:
            return self._row_count(
                client.load_table_from_uri(
                    path,
                    f"{schema}.{table}",
                    job_config=self._job_config(bigquery.SourceFormat.PARQUET),
                )
            )

    @override
    def load_rows(
        self, rows: Sequence[Mapping[str, Any]], *, schema: str, table: str
    ) -> int:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rows.parquet"
            pq.write_table(pa.Table.from_pylist(list(rows)), path)
            return self._load_file(path, schema=schema, table=table)

    @override
    def load_csv_as_strings(self, *, path: str, schema: str, table: str) -> int:
        with UPath(path).open(encoding="utf-8-sig", newline="") as file:
            header = next(csv.reader(file))
        job_config = self._job_config(
            bigquery.SourceFormat.CSV,
            schema=[bigquery.SchemaField(name, "STRING") for name in header],
            skip_leading_rows=1,
            allow_quoted_newlines=True,
        )
        with self._client() as client:
            return self._row_count(
                client.load_table_from_uri(
                    path, f"{schema}.{table}", job_config=job_config
                )
            )

    @override
    def get_rows(self, model: str) -> list[dict[str, Any]]:
        with self._client() as client:
            return client.query(f"select * from {model}").to_arrow().to_pylist()

    @override
    def write_csv(
        self,
        model: str,
        *,
        directory: str,
        file_stem: str,
        partition_column: str | None = None,
    ) -> int:
        raise NotImplementedError("Exporting from BigQuery is tracked in pipelines#165")
