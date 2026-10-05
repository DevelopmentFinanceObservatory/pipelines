import dagster as dg
import pyarrow.parquet as pq
from dagster_duckdb import DuckDBResource
from oda_reader import bulk_download_dac2b
from oda_reader.dac2b import DAC2B_BULK_LABEL
from oda_reader.download.download_tools import (
    DAC2B_FLOW_URL,
    get_bulk_file_url_with_version,
)

from dfo_pipelines.defs.automation import EAGER_WHEN_CHECKS_PASS
from dfo_pipelines.defs.resources import UPathFileStoreResource


@dg.observable_source_asset(
    group_name="oecd_dac_2b",
    automation_condition=dg.AutomationCondition.on_cron("@daily"),
)
def oecd_dac_2b_source() -> dg.ObserveResult:
    """Observed OECD DAC 2b data version.

    The source is observed daily via calls to the OECD API.
    """
    # bypass_annotation_cache defeats oda_reader's 7-day on-disk HTTP cache,
    # which would otherwise let a daily observation serve a version token up to
    # a week old. Refreshing that cache entry is also what lets the
    # bulk_download_dac2b call in oecd_dac_2b_raw_file see the new version, since
    # it re-resolves the URL itself without a bypass.
    url, version = get_bulk_file_url_with_version(
        flow_url=DAC2B_FLOW_URL,
        label=DAC2B_BULK_LABEL,
        bypass_annotation_cache=True,
    )

    if version is None:
        raise ValueError(f"DAC2b version check: could not get version for {url}")

    data_version = dg.DataVersion(";".join([url, version]))

    return dg.ObserveResult(
        metadata={
            "url": dg.MetadataValue.text(url),
            "version": dg.MetadataValue.text(version),
        },
        data_version=data_version,
    )


@dg.asset(
    deps=[oecd_dac_2b_source],
    group_name="oecd_dac_2b",
    kinds={"file", "parquet"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    check_specs=[
        dg.AssetCheckSpec(
            name="file_not_empty", asset="oecd_dac_2b_raw_file", blocking=True
        )
    ],
)
def oecd_dac_2b_raw_file(
    context: dg.AssetExecutionContext, raw_file_store: UPathFileStoreResource
):
    """Raw OECD DAC2b bulk download file"""
    directory = raw_file_store.build_date_partitioned_path("oecd_dac_2b/dac_2b")
    directory.mkdir(parents=True, exist_ok=True)
    file_path = directory / "data.parquet"
    tmp_path = directory / "table2b_data.parquet"
    bulk_download_dac2b(save_to_path=str(directory), use_raw_cache=False)
    tmp_path.rename(file_path)
    rows = pq.read_metadata(file_path).num_rows
    file_size = file_path.stat().st_size
    context.log.info(f"Wrote OECD DAC2b data to {file_path}")
    return dg.MaterializeResult(
        value=str(file_path),
        metadata={
            "dagster/uri": dg.MetadataValue.text(str(file_path)),
            "file_size": dg.MetadataValue.int(file_size),
        },
        check_results=[
            dg.AssetCheckResult(check_name="file_not_empty", passed=(rows > 0))
        ],
    )


@dg.asset(
    group_name="oecd_dac_2b",
    kinds={"duckdb"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    check_specs=[
        dg.AssetCheckSpec(
            name="table_not_empty", asset="oecd_dac_2b_raw_table", blocking=True
        )
    ],
)
def oecd_dac_2b_raw_table(
    duckdb: DuckDBResource, oecd_dac_2b_raw_file: str
) -> dg.MaterializeResult:
    """Raw OECD DAC 2b database table"""
    schema = "oecd_dac_2b"
    table = "raw_dac_2b"
    published = pq.read_schema(oecd_dac_2b_raw_file).names
    renames = {
        "RECIPIENT": "recipient_code",
        "Recipient": "recipient_name",
        "DONOR": "donor_code",
        "Donor": "donor_name",
        "PART": "part_code",
        "Part": "part_name",
    }
    aliases = [renames.get(name, name) for name in published]
    columns = ", ".join('"' + alias.replace('"', '""') + '"' for alias in aliases)
    with duckdb.get_connection() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(
            f"CREATE OR REPLACE TABLE {schema}.{table} AS "
            f"SELECT * FROM read_parquet(?) AS source({columns})",
            [oecd_dac_2b_raw_file],
        )
        rows = (conn.sql(f"SELECT COUNT(*) FROM {schema}.{table}").fetchall())[0][0]
    return dg.MaterializeResult(
        metadata={
            "dagster/table_name": dg.MetadataValue.text(f"{schema}.{table}"),
            "dagster/row_count": dg.MetadataValue.int(rows),
        },
        check_results=[
            dg.AssetCheckResult(check_name="table_not_empty", passed=(rows > 0))
        ],
    )
