from collections.abc import Collection, Mapping

import dagster as dg
from dagster_duckdb import DuckDBResource
from upath import UPath

from dfo_pipelines.orchestration.automation import EAGER_WHEN_CHECKS_PASS
from dfo_pipelines.warehouse import load_extraction_metadata, load_parquet


def raw_table_result(
    table_name: str, rows: int, *, asset_key: dg.AssetKey | None = None
) -> dg.MaterializeResult:
    """Result of loading `table_name`, with a check that it holds rows"""
    return dg.MaterializeResult(
        asset_key=asset_key,
        metadata=_table_metadata(table_name, rows),
        check_results=[
            dg.AssetCheckResult(
                asset_key=asset_key, check_name="table_not_empty", passed=(rows > 0)
            )
        ],
    )


def metadata_table_result(table_name: str, rows: int) -> dg.MaterializeResult:
    """Result of loading the extraction metadata into `table_name`"""
    return dg.MaterializeResult(metadata=_table_metadata(table_name, rows))


def _table_metadata(table_name: str, rows: int) -> dict[str, dg.MetadataValue]:
    return {
        "dagster/table_name": dg.MetadataValue.text(table_name),
        "dagster/row_count": dg.MetadataValue.int(rows),
    }


def build_raw_table_assets(
    *,
    group: str,
    table: str,
    title: str,
    renames: Mapping[str, str] | None = None,
    release_names: Collection[str] = ("source",),
) -> tuple[dg.AssetsDefinition, dg.AssetsDefinition]:
    """The `<group>_raw_table` and `<group>_raw_metadata` assets loading `<group>_raw_file`"""
    raw_file_input = dg.AssetIn(f"{group}_raw_file")

    @dg.asset(
        name=f"{group}_raw_table",
        description=f"Raw {title} database table",
        ins={"raw_file": raw_file_input},
        group_name=group,
        kinds={"duckdb"},
        automation_condition=EAGER_WHEN_CHECKS_PASS,
        check_specs=[
            dg.AssetCheckSpec(
                name="table_not_empty", asset=f"{group}_raw_table", blocking=True
            )
        ],
    )
    def raw_table(duckdb: DuckDBResource, raw_file: str) -> dg.MaterializeResult:
        with duckdb.get_connection() as connection:
            rows = load_parquet(
                connection, path=raw_file, schema=group, table=table, renames=renames
            )
        return raw_table_result(f"{group}.{table}", rows)

    @dg.asset(
        name=f"{group}_raw_metadata",
        description=f"Extraction metadata for the {title} raw data",
        ins={"raw_file": raw_file_input},
        group_name=group,
        kinds={"duckdb"},
        automation_condition=EAGER_WHEN_CHECKS_PASS,
    )
    def raw_metadata(duckdb: DuckDBResource, raw_file: str) -> dg.MaterializeResult:
        with duckdb.get_connection() as connection:
            rows = load_extraction_metadata(
                connection,
                directory=UPath(raw_file).parent,
                schema=group,
                table=f"{table}_metadata",
                release_names=release_names,
            )
        return metadata_table_result(f"{group}.{table}_metadata", rows)

    return raw_table, raw_metadata
