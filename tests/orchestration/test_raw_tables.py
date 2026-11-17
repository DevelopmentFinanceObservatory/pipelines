from datetime import UTC, date, datetime
from pathlib import Path

import dagster as dg
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from dagster_duckdb import DuckDBResource
from upath import UPath

from dfo_pipelines.extraction.metadata import write_extraction_metadata
from dfo_pipelines.orchestration.raw_tables import (
    build_raw_table_assets,
    metadata_table_result,
    raw_table_result,
)


def test_raw_table_result_reports_the_table_and_its_row_count() -> None:
    """Test the metadata names the table and counts its rows"""
    result = raw_table_result("raw.table", 7)

    assert result.metadata == {
        "dagster/table_name": dg.TextMetadataValue("raw.table"),
        "dagster/row_count": dg.IntMetadataValue(7),
    }


def test_raw_table_result_check_passes_with_rows() -> None:
    """Test the table_not_empty check passes for a table holding rows"""
    [check] = raw_table_result("raw.table", 1).check_results

    assert check.check_name == "table_not_empty"
    assert check.passed


def test_raw_table_result_check_fails_with_no_rows() -> None:
    """Test the table_not_empty check fails for an empty table"""
    [check] = raw_table_result("raw.table", 0).check_results

    assert check.check_name == "table_not_empty"
    assert not check.passed


def test_raw_table_result_carries_the_asset_key_on_the_result_and_check() -> None:
    """Test a given asset key reaches both the result and its check"""
    key = dg.AssetKey("some_raw_table")

    result = raw_table_result("raw.table", 1, asset_key=key)

    assert result.asset_key == key
    [check] = result.check_results
    assert check.asset_key == key


def test_metadata_table_result_reports_the_table_and_its_row_count() -> None:
    """Test the metadata names the table and counts its rows, with no check"""
    result = metadata_table_result("raw.table_metadata", 1)

    assert result.metadata == {
        "dagster/table_name": dg.TextMetadataValue("raw.table_metadata"),
        "dagster/row_count": dg.IntMetadataValue(1),
    }
    assert not result.check_results


def test_build_raw_table_assets_loads_the_raw_file_into_tables_named_by_the_builder(
    tmp_path: Path,
) -> None:
    """Test the built assets read the raw file asset's output into their own tables"""

    @dg.asset(group_name="example")
    def example_raw_file() -> str:
        directory = tmp_path / "raw"
        directory.mkdir()
        path = directory / "data.parquet"
        pq.write_table(pa.table({"code": [1, 2, 3]}), path)
        write_extraction_metadata(
            UPath(directory),
            downloaded_at=datetime(2026, 1, 1, tzinfo=UTC),
            release_dates={"source": date(2025, 12, 1)},
        )
        return str(path)

    example_raw_table, example_raw_metadata = build_raw_table_assets(
        group="example", table="raw_example", title="Example"
    )
    database = tmp_path / "database.duckdb"

    result = dg.materialize(
        [example_raw_file, example_raw_table, example_raw_metadata],
        resources={"duckdb": DuckDBResource(database=str(database))},
    )

    assert result.success
    with duckdb.connect(str(database), read_only=True) as connection:
        [(raw_rows,)] = connection.execute(
            "select count(*) from example.raw_example"
        ).fetchall()
        [(metadata_rows,)] = connection.execute(
            "select count(*) from example.raw_example_metadata"
        ).fetchall()
    assert (raw_rows, metadata_rows) == (3, 1)
    assert example_raw_table.key == dg.AssetKey("example_raw_table")
    assert example_raw_metadata.key == dg.AssetKey("example_raw_metadata")
    assert example_raw_table.group_names_by_key == {example_raw_table.key: "example"}
    assert example_raw_metadata.group_names_by_key == {
        example_raw_metadata.key: "example"
    }
    assert example_raw_table.descriptions_by_key == {
        example_raw_table.key: "Raw Example database table"
    }
    assert example_raw_metadata.descriptions_by_key == {
        example_raw_metadata.key: "Extraction metadata for the Example raw data"
    }
    [check_spec] = example_raw_table.check_specs
    assert check_spec.name == "table_not_empty"
    assert check_spec.blocking
    assert [
        evaluation.passed for evaluation in result.get_asset_check_evaluations()
    ] == [True]
