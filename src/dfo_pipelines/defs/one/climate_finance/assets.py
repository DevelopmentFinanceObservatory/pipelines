from datetime import UTC, datetime

import dagster as dg
from dagster_duckdb import DuckDBResource
from upath import UPath

from dfo_pipelines.defs.automation import EAGER_WHEN_CHECKS_PASS
from dfo_pipelines.defs.resources import (
    ClimateFinanceExportResource,
    UPathFileStoreResource,
)
from dfo_pipelines.extraction.climate_finance import (
    LATEST_FILE_NAME,
    MANIFEST_FILE_NAME,
    RAW_TABLES,
    ExportManifest,
    LatestExport,
    sha256_bytes,
)
from dfo_pipelines.extraction.metadata import (
    load_extraction_metadata,
    write_extraction_metadata,
)

SCHEMA = "one_climate_finance"
# The asset and database table each raw table loads into, by raw file stem.
RAW_TABLE_KEYS = {
    stem: dg.AssetKey(f"one_climate_finance_raw_{stem}") for stem in RAW_TABLES
}
RAW_TABLE_NAMES = {stem: f"raw_climate_finance_{stem}" for stem in RAW_TABLES}


def read_manifest(directory: UPath, *, sha256: str) -> ExportManifest:
    """The manifest of the export in `directory`, once it has the sha256 `sha256`.

    Raises:
        ValueError: If the manifest has another sha256, or is malformed.
    """
    data = (directory / MANIFEST_FILE_NAME).read_bytes()
    if (actual := sha256_bytes(data)) != sha256:
        raise ValueError(
            f"{directory / MANIFEST_FILE_NAME} has sha256 {actual}, expected {sha256}"
        )
    return ExportManifest.model_validate_json(data)


@dg.observable_source_asset(
    group_name="one_climate_finance",
    automation_condition=dg.AutomationCondition.on_cron("@daily"),
)
def one_climate_finance_source(
    climate_finance_export: ClimateFinanceExportResource,
) -> dg.ObserveResult:
    """Observed export of the latest climate-finance release.

    The export root's `latest.json` names the release and the sha256 of its
    manifest, which the data version is made of. The manifest records the sha256
    of each raw table, so any change to the export moves the data version.

    Raises:
        RuntimeError: If `latest.json` or the manifest it names is missing,
            malformed, or the manifest has another sha256.
    """
    root = UPath(climate_finance_export.uri)
    try:
        latest = LatestExport.model_validate_json(
            (root / LATEST_FILE_NAME).read_bytes()
        )
        directory = root / latest.release
        read_manifest(directory, sha256=latest.manifest_sha256)
    except (OSError, ValueError) as error:
        raise RuntimeError(
            f"Could not read the climate finance export at {root}: {error}"
        ) from error
    return dg.ObserveResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(str(directory)),
            "version": dg.MetadataValue.text(latest.release),
            "sha256": dg.MetadataValue.text(latest.manifest_sha256),
        },
        data_version=dg.DataVersion(f"{latest.release};{latest.manifest_sha256}"),
    )


def latest_observation(context: dg.AssetExecutionContext) -> tuple[UPath, str]:
    """The export directory the source asset last observed, and the sha256 of its
    manifest.

    Raises:
        ValueError: If the source has no observation with a data version.
    """
    key = one_climate_finance_source.key
    record = context.instance.get_latest_data_version_record(key, is_source=True)
    observation = record.asset_observation if record else None
    if observation is None or observation.data_version is None:
        raise ValueError(f"{key.to_user_string()} has no observation; observe it first")
    metadata = observation.metadata
    return UPath(str(metadata["dagster/uri"].value)), str(metadata["sha256"].value)


@dg.asset(
    deps=[one_climate_finance_source],
    group_name="one_climate_finance",
    kinds={"file", "parquet"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    check_specs=[
        dg.AssetCheckSpec(
            name=f"{stem}_not_empty",
            asset="one_climate_finance_raw_file",
            blocking=True,
        )
        for stem in RAW_TABLES
    ],
)
# The function has no return annotation because Dagster types an asset annotated
# `-> MaterializeResult` as returning Nothing, and the directory this returns is
# the raw tables' input.
def one_climate_finance_raw_file(
    context: dg.AssetExecutionContext, raw_file_store: UPathFileStoreResource
):
    """Raw climate finance from the observed climate-finance release, one parquet
    file per raw table, copied from its export.

    `spending` holds the package's climate finance received, 2010 to 2024, and
    provided, 2013 to 2024, under ONE Data's and the OECD's methods and both
    attribution rules, by `bi_multi`. `crdf_commitments` holds the OECD's CRDF
    commitments, 2010 to 2024, and `btr_totals` the parties' Biennial Transparency
    Report totals. `scripts/export_climate_finance_release.py` extracts them.

    Raises:
        ValueError: If the export's manifest or a table differs from the sha256
            the observation or the manifest recorded.
    """
    downloaded_at = datetime.now(UTC)
    export, manifest_sha256 = latest_observation(context)
    manifest = read_manifest(export, sha256=manifest_sha256)
    directory = raw_file_store.build_date_partitioned_path(
        "one_climate_finance/climate_finance", partition_date=downloaded_at.date()
    )
    directory.mkdir(parents=True, exist_ok=True)

    rows = {}
    file_size = 0
    for stem in RAW_TABLES:
        source = export / f"{stem}.parquet"
        data = source.read_bytes()
        if (actual := sha256_bytes(data)) != (expected := manifest.tables[stem].sha256):
            raise ValueError(
                f"{source} has sha256 {actual}, its manifest records {expected}"
            )
        (directory / f"{stem}.parquet").write_bytes(data)
        file_size += len(data)
        rows[stem] = manifest.tables[stem].rows

    release_date = manifest.created_at.date()
    write_extraction_metadata(
        directory, downloaded_at=downloaded_at, release_dates={"source": release_date}
    )
    context.log.info(
        f"Copied climate finance release {manifest.release} to {directory}"
    )
    return dg.MaterializeResult(
        value=str(directory),
        metadata={
            "dagster/uri": dg.MetadataValue.text(str(directory)),
            "file_size": dg.MetadataValue.int(file_size),
            "dagster/row_count": dg.MetadataValue.int(sum(rows.values())),
            **{
                f"{stem}_row_count": dg.MetadataValue.int(count)
                for stem, count in rows.items()
            },
            "version": dg.MetadataValue.text(manifest.release),
            "source_release_date": dg.MetadataValue.text(release_date.isoformat()),
        },
        check_results=[
            dg.AssetCheckResult(check_name=f"{stem}_not_empty", passed=count > 0)
            for stem, count in rows.items()
        ],
    )


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            key,
            description=f"Raw climate finance {stem} database table",
            group_name="one_climate_finance",
            kinds={"duckdb"},
            automation_condition=EAGER_WHEN_CHECKS_PASS,
            deps=[one_climate_finance_raw_file],
        )
        for stem, key in RAW_TABLE_KEYS.items()
    ],
    ins={"one_climate_finance_raw_file": dg.AssetIn()},
    check_specs=[
        dg.AssetCheckSpec(name="table_not_empty", asset=key, blocking=True)
        for key in RAW_TABLE_KEYS.values()
    ],
)
def one_climate_finance_raw_tables(
    duckdb: DuckDBResource, one_climate_finance_raw_file: str
):
    """Raw climate finance database tables, one per raw file"""
    with duckdb.get_connection() as connection:
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        for stem, key in RAW_TABLE_KEYS.items():
            table = f"{SCHEMA}.{RAW_TABLE_NAMES[stem]}"
            connection.execute(
                f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_parquet(?)",
                [str(UPath(one_climate_finance_raw_file) / f"{stem}.parquet")],
            )
            rows = connection.sql(f"SELECT COUNT(*) FROM {table}").fetchall()[0][0]
            yield dg.MaterializeResult(
                asset_key=key,
                metadata={
                    "dagster/table_name": dg.MetadataValue.text(table),
                    "dagster/row_count": dg.MetadataValue.int(rows),
                },
                check_results=[
                    dg.AssetCheckResult(
                        asset_key=key, check_name="table_not_empty", passed=rows > 0
                    )
                ],
            )


@dg.asset(
    group_name="one_climate_finance",
    kinds={"duckdb"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
)
def one_climate_finance_raw_metadata(
    duckdb: DuckDBResource, one_climate_finance_raw_file: str
) -> dg.MaterializeResult:
    """Extraction metadata for the climate finance raw data"""
    table = "raw_climate_finance_metadata"
    with duckdb.get_connection() as connection:
        rows = load_extraction_metadata(
            connection,
            directory=UPath(one_climate_finance_raw_file),
            schema=SCHEMA,
            table=table,
            release_names=("source",),
        )
    return dg.MaterializeResult(
        metadata={
            "dagster/table_name": dg.MetadataValue.text(f"{SCHEMA}.{table}"),
            "dagster/row_count": dg.MetadataValue.int(rows),
        },
    )
