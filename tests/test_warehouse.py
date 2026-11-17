from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from upath import UPath

from dfo_pipelines.extraction.metadata import (
    METADATA_FILE_NAME,
    write_extraction_metadata,
)
from dfo_pipelines.warehouse import load_extraction_metadata, load_parquet

COLUMNS = ("code", "name", "value")
DOWNLOADED_AT = datetime(2026, 8, 3, 23, 47, 5, tzinfo=UTC)
RELEASE_DATES = {"crs": date(2026, 8, 3), "multisystem": date(2026, 7, 15)}


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


def test_load_extraction_metadata_replaces_the_previous_row(tmp_path: Path) -> None:
    """Test a rerun leaves one row, holding what it recorded"""
    connection = duckdb.connect()
    write_extraction_metadata(
        UPath(tmp_path), downloaded_at=DOWNLOADED_AT, release_dates=RELEASE_DATES
    )
    load_extraction_metadata(
        connection,
        directory=UPath(tmp_path),
        schema="raw",
        table="metadata",
        release_names=("crs", "multisystem"),
    )

    later = datetime(2026, 9, 8, 11, 2, tzinfo=UTC)
    later_release_dates = {"crs": date(2026, 9, 7), "multisystem": date(2026, 9, 1)}
    write_extraction_metadata(
        UPath(tmp_path), downloaded_at=later, release_dates=later_release_dates
    )
    rows = load_extraction_metadata(
        connection,
        directory=UPath(tmp_path),
        schema="raw",
        table="metadata",
        release_names=("crs", "multisystem"),
    )

    assert rows == 1
    assert connection.sql(
        "select downloaded_at, crs_release_date, multisystem_release_date "
        "from raw.metadata"
    ).fetchall() == [
        (later, later_release_dates["crs"], later_release_dates["multisystem"])
    ]


def test_load_extraction_metadata_creates_columns_in_the_given_order(
    tmp_path: Path,
) -> None:
    """Test the release date columns follow the order release_names is given in"""
    connection = duckdb.connect()
    write_extraction_metadata(
        UPath(tmp_path), downloaded_at=DOWNLOADED_AT, release_dates=RELEASE_DATES
    )
    load_extraction_metadata(
        connection,
        directory=UPath(tmp_path),
        schema="raw",
        table="metadata",
        release_names=("multisystem", "crs"),
    )

    assert [row[0] for row in connection.sql("describe raw.metadata").fetchall()] == [
        "downloaded_at",
        "multisystem_release_date",
        "crs_release_date",
    ]


def test_load_extraction_metadata_raises_when_nothing_was_recorded(
    tmp_path: Path,
) -> None:
    """Test an unrecorded extraction fails rather than loading an empty table"""
    connection = duckdb.connect()

    with pytest.raises(FileNotFoundError, match=METADATA_FILE_NAME):
        load_extraction_metadata(
            connection,
            directory=UPath(tmp_path),
            schema="raw",
            table="metadata",
            release_names=("source",),
        )


def test_load_extraction_metadata_raises_on_an_invalid_release_name(
    tmp_path: Path,
) -> None:
    """Test a release name that is not a safe SQL identifier is refused"""
    connection = duckdb.connect()
    write_extraction_metadata(
        UPath(tmp_path),
        downloaded_at=DOWNLOADED_AT,
        release_dates={"Crs Source": date(2026, 8, 3)},
    )

    with pytest.raises(ValueError, match="Crs Source"):
        load_extraction_metadata(
            connection,
            directory=UPath(tmp_path),
            schema="raw",
            table="metadata",
            release_names=("Crs Source",),
        )
