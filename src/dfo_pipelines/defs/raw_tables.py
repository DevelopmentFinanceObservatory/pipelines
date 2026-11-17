import dagster as dg


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
