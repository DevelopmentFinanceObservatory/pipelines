from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass

import dagster as dg
from dagster_duckdb import DuckDBResource
from upath import UPath

from dfo_pipelines.extraction.metadata import read_extraction_metadata
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


def metadata_table_result(
    table_name: str, rows: int, *, asset_key: dg.AssetKey | None = None
) -> dg.MaterializeResult:
    """Result of loading the extraction metadata into `table_name`"""
    return dg.MaterializeResult(
        asset_key=asset_key, metadata=_table_metadata(table_name, rows)
    )


def _table_metadata(table_name: str, rows: int) -> dict[str, dg.MetadataValue]:
    return {
        "dagster/table_name": dg.MetadataValue.text(table_name),
        "dagster/row_count": dg.MetadataValue.int(rows),
    }


@dataclass(frozen=True, kw_only=True)
class RawTableSpec:
    """A file in a source's landing directory and the database table it loads into

    Attributes:
        asset: Name of the table's asset.
        file_stem: Stem of the parquet file in the landing directory.
        table: Name of the database table, in the source group's schema.
        renames: New name of each column renamed on loading, by its name in the file.
    """

    asset: str
    file_stem: str
    table: str
    renames: Mapping[str, str] | None = None


def build_raw_table_assets(
    *,
    group: str,
    title: str,
    tables: Sequence[RawTableSpec],
    metadata_table: str,
    release_names: Collection[str] = ("source",),
) -> dg.AssetsDefinition:
    """Assets loading the `<group>_landing` asset into database tables

    Validates the extraction metadata's release names, loads each of `tables`, whose
    asset has a blocking check that the table holds rows, then loads the metadata.

    Args:
        group: Group of the source, naming the landing asset and the schema.
        title: Name of the source in the descriptions, such as "OECD DAC1".
        tables: The landing directory's files and the tables they load into.
        metadata_table: Name of the extraction metadata's database table.
        release_names: Names of the releases the metadata records a date for.
    """
    landing_key = dg.AssetKey(f"{group}_landing")
    metadata_key = dg.AssetKey(f"{group}_raw_metadata")

    @dg.multi_asset(
        name=f"{group}_raw_tables",
        specs=[
            *(
                dg.AssetSpec(
                    raw_table.asset,
                    description=f"Raw {title} table loaded from {raw_table.file_stem}.parquet",
                    group_name=group,
                    kinds={"duckdb"},
                    automation_condition=EAGER_WHEN_CHECKS_PASS,
                    deps=[landing_key],
                )
                for raw_table in tables
            ),
            dg.AssetSpec(
                metadata_key,
                description=f"Extraction metadata for the {title} raw data",
                group_name=group,
                kinds={"duckdb"},
                automation_condition=EAGER_WHEN_CHECKS_PASS,
                deps=[landing_key],
            ),
        ],
        ins={"landing_directory": dg.AssetIn(landing_key)},
        check_specs=[
            dg.AssetCheckSpec(
                name="table_not_empty", asset=raw_table.asset, blocking=True
            )
            for raw_table in tables
        ],
    )
    def raw_tables(duckdb: DuckDBResource, landing_directory: str):
        directory = UPath(landing_directory)
        with duckdb.get_connection() as connection:
            read_extraction_metadata(directory, release_names=release_names)
            for raw_table in tables:
                rows = load_parquet(
                    connection,
                    path=str(directory / f"{raw_table.file_stem}.parquet"),
                    schema=group,
                    table=raw_table.table,
                    renames=raw_table.renames,
                )
                yield raw_table_result(
                    f"{group}.{raw_table.table}",
                    rows,
                    asset_key=dg.AssetKey(raw_table.asset),
                )
            rows = load_extraction_metadata(
                connection,
                directory=directory,
                schema=group,
                table=metadata_table,
                release_names=release_names,
            )
            yield metadata_table_result(
                f"{group}.{metadata_table}", rows, asset_key=metadata_key
            )

    return raw_tables
