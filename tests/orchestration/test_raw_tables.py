from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from types import MappingProxyType

import dagster as dg
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dagster_duckdb import DuckDBResource
from upath import UPath

from dfo_pipelines.extraction.metadata import (
    METADATA_FILE_NAME,
    write_extraction_metadata,
)
from dfo_pipelines.orchestration.automation import EAGER_WHEN_CHECKS_PASS
from dfo_pipelines.orchestration.raw_tables import (
    RawTableSpec,
    build_raw_table_assets,
    metadata_table_result,
    raw_table_result,
)
from dfo_pipelines.resources.warehouse import DuckDBWarehouseResource


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


def test_metadata_table_result_reports_the_table_and_its_row_count() -> None:
    """Test the metadata names the table and counts its rows, with no check"""
    result = metadata_table_result("raw.table_metadata", 1)

    assert result.metadata == {
        "dagster/table_name": dg.TextMetadataValue("raw.table_metadata"),
        "dagster/row_count": dg.IntMetadataValue(1),
    }
    assert not result.check_results


DOWNLOADED_AT = datetime(2026, 1, 1, tzinfo=UTC)

TABLES = [
    RawTableSpec(asset="example_raw_things", file_stem="things", table="raw_things"),
    RawTableSpec(
        asset="example_raw_others",
        file_stem="others",
        table="raw_others",
        renames={"Code": "code"},
    ),
]


def build_example() -> dg.AssetsDefinition:
    return build_raw_table_assets(
        group="example",
        title="Example",
        tables=TABLES,
        metadata_table="raw_example_metadata",
        release_names=("crs", "multisystem"),
    )


def write_landing_directory(
    directory: Path,
    *,
    tables: Mapping[str, pa.Table],
    downloaded_at: datetime = datetime(2026, 8, 3, 12, tzinfo=UTC),
    release_dates: Mapping[str, date] = MappingProxyType({"source": date(2026, 7, 7)}),
) -> Path:
    """A landing directory holding each of `tables` as a parquet file under its stem,
    with the extraction metadata"""
    directory.mkdir(parents=True)
    for stem, table in tables.items():
        pq.write_table(table, directory / f"{stem}.parquet")
    write_extraction_metadata(
        UPath(directory),
        downloaded_at=downloaded_at,
        run_id="example-run",
        release_dates=release_dates,
    )
    return directory


def load_raw_tables(
    assets: dg.AssetsDefinition,
    *,
    duckdb: DuckDBResource,
    directory: Path,
    raise_on_error: bool = True,
) -> dg.ExecuteInProcessResult:
    """Run the raw tables in a run whose landing asset returned `directory`"""
    [landing_key] = assets.keys_by_input_name.values()

    @dg.asset(key=landing_key)
    def landing() -> str:
        return str(directory)

    return dg.materialize(
        [landing, assets],
        resources={"warehouse": DuckDBWarehouseResource(duckdb=duckdb)},
        raise_on_error=raise_on_error,
    )


def write_landing_directory_of_rows(directory: Path, rows: dict[str, int]) -> None:
    """Write a parquet file of `rows[stem]` rows for each stem, and the metadata"""
    write_landing_directory(
        directory,
        tables={
            stem: pa.table({"Code": list(range(count))}) for stem, count in rows.items()
        },
        downloaded_at=DOWNLOADED_AT,
        release_dates={"crs": date(2025, 12, 1), "multisystem": date(2025, 11, 1)},
    )


def test_build_raw_table_assets_loads_each_file_and_the_metadata(
    tmp_path: Path,
) -> None:
    """Test each file loads, renamed, into its table, and the metadata records each
    release, with the table name and rows on each asset"""
    database = tmp_path / "database.duckdb"
    duckdb_resource = DuckDBResource(database=str(database))
    assets = build_example()
    write_landing_directory_of_rows(tmp_path / "raw", {"things": 3, "others": 2})

    result = load_raw_tables(assets, duckdb=duckdb_resource, directory=tmp_path / "raw")

    assert result.success
    with duckdb.connect(str(database), read_only=True) as connection:
        things = connection.sql("select * from example.raw_things")
        others = connection.sql("select * from example.raw_others")
        metadata = connection.sql("select * from example.raw_example_metadata")
        assert (things.columns, len(things.fetchall())) == (["Code"], 3)
        assert (others.columns, len(others.fetchall())) == (["code"], 2)
        assert metadata.fetchall() == [
            (DOWNLOADED_AT, date(2025, 12, 1), date(2025, 11, 1))
        ]
    assert {
        materialization.asset_key: materialization.metadata
        for materialization in result.asset_materializations_for_node(
            "example_raw_tables"
        )
    } == {
        dg.AssetKey(asset): {
            "dagster/table_name": dg.TextMetadataValue(f"example.{table}"),
            "dagster/row_count": dg.IntMetadataValue(rows),
        }
        for asset, table, rows in [
            ("example_raw_things", "raw_things", 3),
            ("example_raw_others", "raw_others", 2),
            ("example_raw_metadata", "raw_example_metadata", 1),
        ]
    }
    assert all(check.passed for check in result.get_asset_check_evaluations())


def test_build_raw_table_assets_check_fails_only_on_the_empty_table(
    tmp_path: Path,
) -> None:
    """Test the blocking check of the table loaded from an empty file fails, and
    only that one"""
    write_landing_directory_of_rows(tmp_path / "raw", {"things": 3, "others": 0})

    result = load_raw_tables(
        build_example(),
        duckdb=DuckDBResource(database=str(tmp_path / "database.duckdb")),
        directory=tmp_path / "raw",
        raise_on_error=False,
    )

    assert not result.success
    assert {
        check.asset_key: check.passed for check in result.get_asset_check_evaluations()
    } == {
        dg.AssetKey("example_raw_things"): True,
        dg.AssetKey("example_raw_others"): False,
    }


def test_build_raw_table_assets_failed_table_load_records_no_metadata(
    tmp_path: Path,
) -> None:
    """Test a table whose file is missing fails the run with no materialization of
    the metadata asset"""
    write_landing_directory_of_rows(tmp_path / "raw", {"things": 3})

    result = load_raw_tables(
        build_example(),
        duckdb=DuckDBResource(database=str(tmp_path / "database.duckdb")),
        directory=tmp_path / "raw",
        raise_on_error=False,
    )

    assert not result.success
    assert dg.AssetKey("example_raw_metadata") not in {
        materialization.asset_key
        for materialization in result.asset_materializations_for_node(
            "example_raw_tables"
        )
    }


def test_build_raw_table_assets_refuses_an_incomplete_directory_naming_its_landing(
    tmp_path: Path,
) -> None:
    """Test a directory without extraction metadata fails the run before any table
    loads, naming the landing asset to materialize"""
    write_landing_directory_of_rows(tmp_path / "raw", {"things": 3, "others": 2})
    (tmp_path / "raw" / METADATA_FILE_NAME).unlink()

    result = load_raw_tables(
        build_example(),
        duckdb=DuckDBResource(database=str(tmp_path / "database.duckdb")),
        directory=tmp_path / "raw",
        raise_on_error=False,
    )

    failure = result.failure_data_for_node("example_raw_tables")
    assert failure is not None
    assert failure.error is not None
    assert "materialize example_landing" in failure.error.to_string()
    assert not result.asset_materializations_for_node("example_raw_tables")


def test_build_raw_table_assets_refuses_a_release_name_that_is_not_an_identifier() -> (
    None
):
    """Test a release name that is not a safe SQL identifier is refused when the
    assets are defined"""
    with pytest.raises(ValueError, match="Crs Source"):
        build_raw_table_assets(
            group="example",
            title="Example",
            tables=TABLES,
            metadata_table="raw_example_metadata",
            release_names=("crs", "Crs Source"),
        )


def test_build_raw_table_assets_mismatched_release_names_load_no_table(
    tmp_path: Path,
) -> None:
    """Test metadata recording other releases than the assets expect fails the run
    before any table loads"""
    duckdb_resource = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    write_landing_directory(
        tmp_path / "raw",
        tables={"things": pa.table({"Code": [1]}), "others": pa.table({"Code": [1]})},
        release_dates={"source": date(2025, 12, 1)},
    )

    result = load_raw_tables(
        build_example(),
        duckdb=duckdb_resource,
        directory=tmp_path / "raw",
        raise_on_error=False,
    )

    assert not result.success
    assert not result.asset_materializations_for_node("example_raw_tables")
    # The failed run leaves its read-write connection open in this process, so a
    # read-only one would clash with it.
    with duckdb_resource.get_connection() as connection:
        assert not connection.sql("select * from information_schema.tables").fetchall()


def test_build_raw_table_assets_defines_the_assets_downstream_of_the_directory() -> (
    None
):
    """Test each asset's dep, group, description, automation and blocking checks"""
    assets = build_example()

    landing = dg.AssetKey("example_landing")
    assert assets.keys_by_input_name == {"landing_directory": landing}
    assert {key: spec.description for key, spec in assets.specs_by_key.items()} == {
        dg.AssetKey(
            "example_raw_things"
        ): "Raw Example table loaded from things.parquet",
        dg.AssetKey(
            "example_raw_others"
        ): "Raw Example table loaded from others.parquet",
        dg.AssetKey(
            "example_raw_metadata"
        ): "Extraction metadata for the Example raw data",
    }
    for spec in assets.specs:
        assert [dep.asset_key for dep in spec.deps] == [landing]
        assert spec.group_name == "example"
        assert spec.kinds == {"duckdb"}
        assert spec.automation_condition == EAGER_WHEN_CHECKS_PASS
    assert {
        (check_spec.asset_key, check_spec.name, check_spec.blocking)
        for check_spec in assets.check_specs
    } == {
        (dg.AssetKey("example_raw_things"), "table_not_empty", True),
        (dg.AssetKey("example_raw_others"), "table_not_empty", True),
    }
