import dagster as dg

from dfo_pipelines.defs.raw_tables import metadata_table_result, raw_table_result


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
