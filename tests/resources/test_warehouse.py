from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dagster_duckdb import DuckDBResource

from dfo_pipelines.extraction.metadata import ExtractionMetadata
from dfo_pipelines.resources.warehouse import (
    DuckDBWarehouseResource,
    WarehouseResource,
)

COLUMNS = ("code", "name", "value")
DOWNLOADED_AT = datetime(2026, 8, 3, 23, 47, 5, tzinfo=UTC)
RELEASE_DATES = {"crs": date(2026, 8, 3), "multisystem": date(2026, 7, 15)}


@pytest.fixture(params=["duckdb"])
def warehouse(request: pytest.FixtureRequest, tmp_path: Path) -> WarehouseResource:
    """An empty warehouse of each implementation"""
    match request.param:
        case "duckdb":
            return DuckDBWarehouseResource(
                duckdb=DuckDBResource(database=str(tmp_path / "database.duckdb"))
            )
        case _:
            raise ValueError(request.param)


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


def _load_dated_rows(warehouse: WarehouseResource, tmp_path: Path) -> None:
    """Load `raw.t` with two rows for `date` 2001, out of order, and one for 2000"""
    path = tmp_path / "partitioned.parquet"
    pq.write_table(
        pa.table(
            {
                "date": [2001, 2000, 2001],
                "variable": ["b", "c", "a"],
                "value": [2, 3, 1],
            }
        ),
        path,
    )
    warehouse.load_parquet(path=str(path), schema="raw", table="t")


def test_load_parquet_loads_every_column_and_row(
    warehouse: WarehouseResource, tmp_path: Path
) -> None:
    """Test the whole file lands in the table, and the row count is returned"""
    rows = warehouse.load_parquet(
        path=_write(tmp_path / "data.parquet", 3), schema="raw", table="t"
    )

    assert rows == 3
    assert warehouse.read_table("raw.t").to_pylist() == [
        {"code": 0, "name": "name 0", "value": 0},
        {"code": 1, "name": "name 1", "value": 10},
        {"code": 2, "name": "name 2", "value": 20},
    ]


def test_load_parquet_loads_an_empty_file_as_an_empty_table(
    warehouse: WarehouseResource, tmp_path: Path
) -> None:
    """Test a file with no rows gives an empty table with its columns, and 0"""
    rows = warehouse.load_parquet(
        path=_write(tmp_path / "empty.parquet", 0), schema="raw", table="t"
    )

    assert rows == 0
    assert warehouse.read_table("raw.t").column_names == list(COLUMNS)


def test_load_parquet_replaces_an_existing_table(
    warehouse: WarehouseResource, tmp_path: Path
) -> None:
    """Test a reload leaves only the rows of the new file"""
    warehouse.load_parquet(
        path=_write(tmp_path / "big.parquet", 5), schema="raw", table="t"
    )

    rows = warehouse.load_parquet(
        path=_write(tmp_path / "small.parquet", 2), schema="raw", table="t"
    )

    assert rows == 2
    assert len(warehouse.get_rows("raw.t")) == 2


def test_load_parquet_aliases_columns_by_position(
    warehouse: WarehouseResource, tmp_path: Path
) -> None:
    """Test renames reach columns whose names differ only by case, others keep
    their name and the order is unchanged"""
    warehouse.load_parquet(
        path=_write(tmp_path / "data.parquet", 2, ("DONOR", "Donor", "value")),
        schema="raw",
        table="t",
        renames={"DONOR": "donor_code", "Donor": "donor_name"},
    )

    assert warehouse.read_table("raw.t").to_pylist() == [
        {"donor_code": 0, "donor_name": "name 0", "value": 0},
        {"donor_code": 1, "donor_name": "name 1", "value": 10},
    ]


def test_load_extraction_metadata_replaces_the_previous_row(
    warehouse: WarehouseResource,
) -> None:
    """Test a rerun leaves one row, holding what it was given"""
    warehouse.load_extraction_metadata(
        metadata=ExtractionMetadata(
            downloaded_at=DOWNLOADED_AT, release_dates=RELEASE_DATES
        ),
        schema="raw",
        table="metadata",
    )

    later = datetime(2026, 9, 8, 11, 2, tzinfo=UTC)
    later_release_dates = {"crs": date(2026, 9, 7), "multisystem": date(2026, 9, 1)}
    rows = warehouse.load_extraction_metadata(
        metadata=ExtractionMetadata(
            downloaded_at=later, release_dates=later_release_dates
        ),
        schema="raw",
        table="metadata",
    )

    assert rows == 1
    assert warehouse.get_rows("raw.metadata") == [
        {
            "downloaded_at": later,
            "crs_release_date": later_release_dates["crs"],
            "multisystem_release_date": later_release_dates["multisystem"],
        }
    ]


def test_load_extraction_metadata_creates_columns_in_the_order_of_the_releases(
    warehouse: WarehouseResource,
) -> None:
    """Test the release date columns follow the order of the metadata's releases"""
    warehouse.load_extraction_metadata(
        metadata=ExtractionMetadata(
            downloaded_at=DOWNLOADED_AT,
            release_dates={
                "multisystem": RELEASE_DATES["multisystem"],
                "crs": RELEASE_DATES["crs"],
            },
        ),
        schema="raw",
        table="metadata",
    )

    [row] = warehouse.get_rows("raw.metadata")
    assert list(row.items()) == [
        ("downloaded_at", DOWNLOADED_AT),
        ("multisystem_release_date", RELEASE_DATES["multisystem"]),
        ("crs_release_date", RELEASE_DATES["crs"]),
    ]


def test_load_csv_as_strings_keeps_every_value_as_written(
    warehouse: WarehouseResource, tmp_path: Path
) -> None:
    """Test a numeric-looking column stays a string, with its leading zero"""
    path = tmp_path / "codes.csv"
    path.write_text("code,label\n007,Seven\n010,Ten\n")

    rows = warehouse.load_csv_as_strings(path=str(path), schema="raw", table="codes")

    assert rows == 2
    assert warehouse.read_table("raw.codes").to_pylist() == [
        {"code": "007", "label": "Seven"},
        {"code": "010", "label": "Ten"},
    ]


def test_load_csv_as_strings_replaces_an_existing_table(
    warehouse: WarehouseResource, tmp_path: Path
) -> None:
    """Test a reload leaves only the rows of the new file"""
    path = tmp_path / "codes.csv"
    path.write_text("code\n1\n2\n3\n")
    warehouse.load_csv_as_strings(path=str(path), schema="raw", table="codes")
    path.write_text("code\n4\n")

    rows = warehouse.load_csv_as_strings(path=str(path), schema="raw", table="codes")

    assert rows == 1
    assert warehouse.get_rows("raw.codes") == [{"code": "4"}]


def test_read_table_sorts_by_every_column(
    warehouse: WarehouseResource, tmp_path: Path
) -> None:
    """Test the rows come back sorted by the first column, then the next"""
    _load_dated_rows(warehouse, tmp_path)

    assert warehouse.read_table("raw.t").to_pylist() == [
        {"date": 2000, "variable": "c", "value": 3},
        {"date": 2001, "variable": "a", "value": 1},
        {"date": 2001, "variable": "b", "value": 2},
    ]


def test_write_partitions_writes_each_values_rows_sorted_to_its_own_file(
    warehouse: WarehouseResource, tmp_path: Path
) -> None:
    """Test each value gets a file named for it holding only its rows, sorted, and
    the total row count is returned"""
    _load_dated_rows(warehouse, tmp_path)
    directory = tmp_path / "bundle"
    directory.mkdir()

    rows = warehouse.write_partitions(
        "raw.t", column="date", directory=str(directory), file_stem="observations"
    )

    assert rows == 3
    assert {path.name: path.read_text() for path in directory.iterdir()} == {
        "observations_2000.csv": "date,variable,value\n2000,c,3\n",
        "observations_2001.csv": "date,variable,value\n2001,a,1\n2001,b,2\n",
    }
