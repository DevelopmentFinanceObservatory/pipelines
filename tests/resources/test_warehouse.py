from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from google.cloud import bigquery

from dfo_pipelines.resources.warehouse import (
    DuckDBWarehouseResource,
    WarehouseResource,
    aliased_column_names,
    arrow_decimal_types,
    write_batches_to_duckdb,
    write_renamed_parquet,
)

COLUMNS = ("code", "name", "value")
DOWNLOADED_AT = datetime(2026, 8, 3, 23, 47, 5, tzinfo=UTC)
RELEASE_DATES = {"crs": date(2026, 8, 3), "multisystem": date(2026, 7, 15)}
_DATED_FIELDS = [("date", pa.int64()), ("variable", pa.string())]


@pytest.fixture(params=["duckdb"])
def warehouse(request: pytest.FixtureRequest, tmp_path: Path) -> WarehouseResource:
    """An empty warehouse of each implementation"""
    match request.param:
        case "duckdb":
            return DuckDBWarehouseResource(database=str(tmp_path / "database.duckdb"))
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
    assert sorted(warehouse.get_rows("raw.t"), key=lambda row: row["code"]) == [
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
    warehouse.write_csv("raw.t", directory=str(tmp_path), file_stem="header")
    assert (tmp_path / "header.csv").read_text() == ",".join(COLUMNS) + "\n"


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

    assert sorted(warehouse.get_rows("raw.t"), key=lambda row: row["donor_code"]) == [
        {"donor_code": 0, "donor_name": "name 0", "value": 0},
        {"donor_code": 1, "donor_name": "name 1", "value": 10},
    ]


def test_load_rows_replaces_the_previous_rows(warehouse: WarehouseResource) -> None:
    """Test a reload leaves only the rows it was given"""
    warehouse.load_rows(
        [{"downloaded_at": DOWNLOADED_AT}, {"downloaded_at": DOWNLOADED_AT}],
        schema="raw",
        table="metadata",
    )
    later = datetime(2026, 9, 8, 11, 2, tzinfo=UTC)

    rows = warehouse.load_rows(
        [{"downloaded_at": later}], schema="raw", table="metadata"
    )

    assert rows == 1
    assert warehouse.get_rows("raw.metadata") == [{"downloaded_at": later}]


def test_load_rows_types_columns_from_their_values_in_the_order_given(
    warehouse: WarehouseResource,
) -> None:
    """Test a timezone-aware instant and a date keep their types, and the columns
    follow the row's keys"""
    row = {
        "downloaded_at": DOWNLOADED_AT,
        "multisystem_release_date": RELEASE_DATES["multisystem"],
        "crs_release_date": RELEASE_DATES["crs"],
    }

    warehouse.load_rows([row], schema="raw", table="metadata")

    [loaded] = warehouse.get_rows("raw.metadata")
    assert list(loaded.items()) == list(row.items())
    assert loaded["downloaded_at"].utcoffset() is not None
    assert type(loaded["crs_release_date"]) is date


def test_load_csv_as_strings_keeps_every_value_as_written(
    warehouse: WarehouseResource, tmp_path: Path
) -> None:
    """Test a numeric-looking column stays a string, with its leading zero"""
    path = tmp_path / "codes.csv"
    path.write_text("code,label\n007,Seven\n010,Ten\n")

    rows = warehouse.load_csv_as_strings(path=str(path), schema="raw", table="codes")

    assert rows == 2
    assert sorted(warehouse.get_rows("raw.codes"), key=lambda row: row["code"]) == [
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


def test_write_csv_writes_every_row_sorted_by_every_column_to_one_file(
    warehouse: WarehouseResource, tmp_path: Path
) -> None:
    """Test one file named for the stem holds the header then every row, sorted by
    the first column, then the next, and the row count is returned"""
    _load_dated_rows(warehouse, tmp_path)
    directory = tmp_path / "out"
    directory.mkdir()

    rows = warehouse.write_csv("raw.t", directory=str(directory), file_stem="t")

    assert rows == 3
    assert {path.name: path.read_text() for path in directory.iterdir()} == {
        "t.csv": "date,variable,value\n2000,c,3\n2001,a,1\n2001,b,2\n"
    }


def test_write_csv_writes_each_partitions_rows_sorted_to_its_own_file(
    warehouse: WarehouseResource, tmp_path: Path
) -> None:
    """Test each value gets a file named for it holding only its rows, sorted, and
    the total row count is returned"""
    _load_dated_rows(warehouse, tmp_path)
    directory = tmp_path / "bundle"
    directory.mkdir()

    rows = warehouse.write_csv(
        "raw.t",
        directory=str(directory),
        file_stem="observations",
        partition_column="date",
    )

    assert rows == 3
    assert {path.name: path.read_text() for path in directory.iterdir()} == {
        "observations_2000.csv": "date,variable,value\n2000,c,3\n",
        "observations_2001.csv": "date,variable,value\n2001,a,1\n2001,b,2\n",
    }


def test_aliased_column_names_renames_by_name_and_keeps_the_rest_in_order() -> None:
    """Test names differing only by case take their own new names, others stay"""
    aliases = aliased_column_names(
        ["DONOR", "Donor", "value"], {"DONOR": "donor_code", "Donor": "donor_name"}
    )

    assert aliases == ["donor_code", "donor_name", "value"]


def test_write_renamed_parquet_keeps_every_row_under_the_new_names(
    tmp_path: Path,
) -> None:
    """Test columns whose names differ only by case are renamed apart, with their
    values and types"""
    destination = tmp_path / "renamed.parquet"

    write_renamed_parquet(
        _write(tmp_path / "data.parquet", 2, ("DONOR", "Donor", "value")),
        destination,
        {"DONOR": "donor_code", "Donor": "donor_name"},
    )

    assert pq.read_table(destination) == pa.table(
        {
            "donor_code": pa.array([0, 1], pa.int64()),
            "donor_name": pa.array(["name 0", "name 1"], pa.string()),
            "value": pa.array([0, 10], pa.int64()),
        }
    )


def test_arrow_decimal_types_follow_the_declared_or_default_precision() -> None:
    """Test a declared precision and scale is kept, an undeclared NUMERIC takes
    BigQuery's default, and other types are left out"""
    fields = [
        bigquery.SchemaField("amount", "NUMERIC", precision=10, scale=2),
        bigquery.SchemaField("share", "NUMERIC"),
        bigquery.SchemaField("total", "BIGNUMERIC", precision=20, scale=4),
        bigquery.SchemaField("count", "INT64"),
    ]

    assert arrow_decimal_types(fields) == {
        "amount": pa.decimal128(10, 2),
        "share": pa.decimal128(38, 9),
        "total": pa.decimal128(20, 4),
    }


def test_arrow_decimal_types_fails_for_an_undeclared_bignumeric() -> None:
    """Test a BIGNUMERIC without a declared precision fails, naming the column"""
    with pytest.raises(ValueError, match="'total'"):
        arrow_decimal_types([bigquery.SchemaField("total", "BIGNUMERIC")])


def test_write_batches_to_duckdb_writes_the_csv_the_duckdb_warehouse_writes(
    tmp_path: Path,
) -> None:
    """Test batches with a wide decimal column give the same partitioned CSV files
    as the same rows loaded into the DuckDB warehouse"""
    rows = {
        "date": [2001, 2000, 2001],
        "variable": ["b", "c", "a"],
        "value": [Decimal("2.50"), Decimal("3.00"), Decimal("1.25")],
    }
    loaded = tmp_path / "loaded.parquet"
    pq.write_table(
        pa.table(rows, pa.schema([*_DATED_FIELDS, ("value", pa.decimal128(10, 2))])),
        loaded,
    )
    reference = DuckDBWarehouseResource(database=str(tmp_path / "reference.duckdb"))
    reference.load_parquet(path=str(loaded), schema="raw", table="t")
    streamed = pa.table(
        rows, pa.schema([*_DATED_FIELDS, ("value", pa.decimal256(76, 38))])
    ).to_batches(max_chunksize=2)
    database = str(tmp_path / "streamed.duckdb")

    write_batches_to_duckdb(
        streamed,
        column_names=list(rows),
        decimal_types={"value": pa.decimal128(10, 2)},
        database=database,
        model="raw.t",
    )

    assert _partitioned_csv(
        DuckDBWarehouseResource(database=database), tmp_path / "streamed"
    ) == _partitioned_csv(reference, tmp_path / "reference")


def test_write_batches_to_duckdb_gives_the_columns_of_an_empty_stream(
    tmp_path: Path,
) -> None:
    """Test no batches still give a table with the columns, for the CSV header"""
    database = str(tmp_path / "empty.duckdb")

    write_batches_to_duckdb(
        [],
        column_names=list(COLUMNS),
        decimal_types={},
        database=database,
        model="raw.t",
    )

    DuckDBWarehouseResource(database=database).write_csv(
        "raw.t", directory=str(tmp_path), file_stem="header"
    )
    assert (tmp_path / "header.csv").read_text() == ",".join(COLUMNS) + "\n"


def _partitioned_csv(warehouse: WarehouseResource, directory: Path) -> dict[str, str]:
    """The CSV files `warehouse` writes for `raw.t` partitioned on `date`"""
    directory.mkdir()
    warehouse.write_csv(
        "raw.t", directory=str(directory), file_stem="t", partition_column="date"
    )
    return {path.name: path.read_text() for path in directory.iterdir()}
