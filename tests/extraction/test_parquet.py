from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from dfo_pipelines.extraction.parquet import load_parquet

COLUMNS = ("code", "name", "value")


def _write(path: Path, rows: int, names: tuple[str, str, str] = COLUMNS) -> str:
    """Write a parquet file of `rows` rows with the three column `names`"""
    code, name, value = names
    pq.write_table(
        pa.table(
            {
                code: pa.array(range(rows), pa.int64()),
                name: pa.array([f"name {n}" for n in range(rows)], pa.string()),
                value: pa.array([n * 10 for n in range(rows)], pa.int64()),
            }
        ),
        path,
    )
    return str(path)


def test_load_parquet_loads_every_column_and_row(tmp_path: Path) -> None:
    """Test the whole file lands in the table, and the row count is returned"""
    connection = duckdb.connect()

    rows = load_parquet(
        connection, path=_write(tmp_path / "data.parquet", 3), schema="raw", table="t"
    )

    assert rows == 3
    assert connection.sql("select * from raw.t").columns == list(COLUMNS)
    assert connection.sql("select * from raw.t order by code").fetchall() == [
        (0, "name 0", 0),
        (1, "name 1", 10),
        (2, "name 2", 20),
    ]


def test_load_parquet_loads_an_empty_file_as_an_empty_table(tmp_path: Path) -> None:
    """Test a file with no rows gives an empty table with its columns, and 0"""
    connection = duckdb.connect()

    rows = load_parquet(
        connection, path=_write(tmp_path / "empty.parquet", 0), schema="raw", table="t"
    )

    assert rows == 0
    assert connection.sql("select * from raw.t").columns == list(COLUMNS)


def test_load_parquet_replaces_an_existing_table(tmp_path: Path) -> None:
    """Test a reload leaves only the rows of the new file"""
    connection = duckdb.connect()
    load_parquet(
        connection, path=_write(tmp_path / "big.parquet", 5), schema="raw", table="t"
    )

    rows = load_parquet(
        connection, path=_write(tmp_path / "small.parquet", 2), schema="raw", table="t"
    )

    assert rows == 2
    assert connection.sql("select count(*) from raw.t").fetchall() == [(2,)]


def test_load_parquet_aliases_columns_by_position(tmp_path: Path) -> None:
    """Test renames reach columns whose names differ only by case, others keep
    their name and the order is unchanged"""
    connection = duckdb.connect()

    load_parquet(
        connection,
        path=_write(tmp_path / "data.parquet", 2, ("DONOR", "Donor", "value")),
        schema="raw",
        table="t",
        renames={"DONOR": "donor_code", "Donor": "donor_name"},
    )

    assert connection.sql("select * from raw.t").columns == [
        "donor_code",
        "donor_name",
        "value",
    ]
    assert connection.sql("select * from raw.t order by donor_code").fetchall() == [
        (0, "name 0", 0),
        (1, "name 1", 10),
    ]
