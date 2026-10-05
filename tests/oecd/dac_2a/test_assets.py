import shutil
from datetime import UTC, date, datetime
from functools import partial
from pathlib import Path
from unittest.mock import patch

import dagster as dg
import pytest
import requests
from dagster_duckdb import DuckDBResource
from oda_reader.download.download_tools import DAC2A_FLOW_URL

from dfo_pipelines.defs.oecd.dac_2a.assets import (
    oecd_dac_2a_raw_file,
    oecd_dac_2a_raw_table,
    oecd_dac_2a_source,
)
from dfo_pipelines.defs.resources import UPathFileStoreResource
from tests.helpers import (
    AutomationHarness,
    column_list,
    list_parquet_columns,
    row_count,
)

FIXTURES = Path(__file__).parent / "fixtures"

# Stand-in for the resolved bulk-download URL: never fetched, the lookup is
# mocked, but joined into the data version so it is pinned in assertions.
DAC2A_TEST_URL = "https://webfs-dcd.oecd.org/data/dac_2a/DAC2a.parquet"


def write_download(
    parquet: Path, save_to_path: str, use_raw_cache: bool = True
) -> None:
    """Write the given fixture to bulk_download_dac2a path"""
    shutil.copyfile(parquet, Path(save_to_path) / "table2a_data.parquet")


def test_dac_2a_source_observe_fn_returns_metadata_and_data_version() -> None:
    """Test the source's observe fn records the URL, version and data version"""
    observe = oecd_dac_2a_source.observe_fn
    assert observe is not None

    with patch(
        "dfo_pipelines.defs.oecd.dac_2a.assets.get_bulk_file_url_with_version",
        autospec=True,
        return_value=(DAC2A_TEST_URL, "v20260801"),
    ) as lookup:
        result = observe()

    # the full DAC2a parquet with a cache-bypassing lookup
    lookup.assert_called_once_with(
        flow_url=DAC2A_FLOW_URL,
        label="DAC2A full dataset (dotStat format)",
        bypass_annotation_cache=True,
    )

    assert isinstance(result, dg.ObserveResult)
    assert result.data_version == dg.DataVersion(f"{DAC2A_TEST_URL};v20260801")
    metadata = result.metadata
    assert metadata is not None
    url_meta = metadata["url"]
    version_meta = metadata["version"]
    assert isinstance(url_meta, dg.TextMetadataValue)
    assert isinstance(version_meta, dg.TextMetadataValue)
    assert url_meta.value == DAC2A_TEST_URL
    assert version_meta.value == "v20260801"


def test_dac_2a_source_observe_fn_raises_when_version_unavailable() -> None:
    """Test the observe fn raises when the lookup returns no version token

    Without a token there is nothing to gate the pipeline on, so the
    observation must fail loudly rather than record an empty data version.
    """
    observe = oecd_dac_2a_source.observe_fn
    assert observe is not None

    with (
        patch(
            "dfo_pipelines.defs.oecd.dac_2a.assets.get_bulk_file_url_with_version",
            autospec=True,
            return_value=(DAC2A_TEST_URL, None),
        ),
        pytest.raises(ValueError, match="DAC2a version check"),
    ):
        observe()


def test_dac_2a_source_observe_fn_raises_when_lookup_fails() -> None:
    """Test an unreachable OECD API surfaces rather than being skipped"""
    observe = oecd_dac_2a_source.observe_fn
    assert observe is not None

    with (
        patch(
            "dfo_pipelines.defs.oecd.dac_2a.assets.get_bulk_file_url_with_version",
            autospec=True,
            side_effect=requests.exceptions.ConnectionError("sdmx unreachable"),
        ),
        pytest.raises(requests.exceptions.ConnectionError, match="sdmx unreachable"),
    ):
        observe()


class _DAC2aAutomation(AutomationHarness):
    """Drives the DAC2a assets against an ephemeral instance, as the daemon does

    Version lookups and downloads are mocked for as long as the context is
    open. Assign to `version` to make the next observation see a republished
    source.
    """

    def __init__(self, tmp_path: Path) -> None:
        super().__init__(
            tmp_path,
            source=oecd_dac_2a_source,
            raw_assets=[oecd_dac_2a_raw_file, oecd_dac_2a_raw_table],
        )
        self.version = "v20260801"

    def _start_mocks(self) -> None:
        self._stack.enter_context(
            patch(
                "dfo_pipelines.defs.oecd.dac_2a.assets.get_bulk_file_url_with_version",
                autospec=True,
                side_effect=lambda **kwargs: (DAC2A_TEST_URL, self.version),
            )
        )
        self._stack.enter_context(
            patch(
                "dfo_pipelines.defs.oecd.dac_2a.assets.bulk_download_dac2a",
                side_effect=partial(write_download, FIXTURES / "data.parquet"),
            )
        )


def test_dac_2a_source_observation_records_an_observation_event(
    tmp_path: Path,
) -> None:
    """Test observing the source records one observation against the source"""

    with _DAC2aAutomation(tmp_path) as automation:
        result = automation.observe()

    assert [
        entry.asset_key
        for entry in result.all_events
        if entry.event_type_value == "ASSET_OBSERVATION"
    ] == [oecd_dac_2a_source.key]


def test_dac_2a_raw_assets_are_not_rerun_when_the_source_is_unchanged(
    tmp_path: Path,
) -> None:
    """Test a repeat observation at an unchanged data version requests no rerun

    This is the point of the new data check: the raw assets must not re-run
    while the OECD has not republished the bulk file.
    """
    with _DAC2aAutomation(tmp_path) as automation:
        automation.observe()
        automation.materialize(oecd_dac_2a_raw_file, oecd_dac_2a_raw_table)
        # A baseline tick, not evidence: every condition is reset on its initial
        # evaluation, so the first tick requests nothing whatever the source did.
        automation.tick()

        automation.observe()

        assert automation.tick() == {
            "oecd_dac_2a_raw_file": 0,
            "oecd_dac_2a_raw_table": 0,
        }


def test_dac_2a_source_is_requested_once_per_daily_cron_tick(tmp_path: Path) -> None:
    """Test the source's condition requests one observation per UTC day

    The 13:00 tick is what pins the cadence: a shorter cron would request an
    observation an hour after the last one. The 00:01 tick proves the condition
    requests an observation at all once the daily tick has passed, and 00:02
    proves it is not requested again before the next one.
    """
    with _DAC2aAutomation(tmp_path) as automation:
        # A baseline tick: every condition is reset on its initial evaluation,
        # so the first tick requests nothing whatever the time is.
        automation.source_tick(at=datetime(2026, 8, 25, 12, 0, tzinfo=UTC))

        assert automation.source_tick(at=datetime(2026, 8, 25, 13, 0, tzinfo=UTC)) == 0
        assert automation.source_tick(at=datetime(2026, 8, 26, 0, 1, tzinfo=UTC)) == 1
        assert automation.source_tick(at=datetime(2026, 8, 26, 0, 2, tzinfo=UTC)) == 0


def test_dac_2a_raw_assets_are_rerun_when_the_source_data_version_changes(
    tmp_path: Path,
) -> None:
    """Test a changed data version requests the raw assets, and only once"""
    with _DAC2aAutomation(tmp_path) as automation:
        automation.observe()
        automation.materialize(oecd_dac_2a_raw_file, oecd_dac_2a_raw_table)
        automation.tick()  # baseline, as above

        automation.version = "v20260901"
        automation.observe()

        # Both are requested on the same tick: eager() counts a dependency that
        # will be requested as updated, so the chain is launched in one run.
        assert automation.tick() == {
            "oecd_dac_2a_raw_file": 1,
            "oecd_dac_2a_raw_table": 1,
        }

        automation.materialize(oecd_dac_2a_raw_file, oecd_dac_2a_raw_table)

        assert automation.tick() == {
            "oecd_dac_2a_raw_file": 0,
            "oecd_dac_2a_raw_table": 0,
        }


def test_dac_2a_raw_file_writes_parquet(tmp_path: Path) -> None:
    """Test DAC 2a raw file asset writes the download to a date-partitioned path"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))

    with (
        patch("dfo_pipelines.defs.resources.date") as mock_date,
        patch(
            "dfo_pipelines.defs.oecd.dac_2a.assets.bulk_download_dac2a",
            side_effect=partial(write_download, FIXTURES / "data.parquet"),
        ) as bulk_download,
    ):
        mock_date.today.return_value = date(2026, 8, 3)
        raw_dir = file_store.build_date_partitioned_path("oecd_dac_2a/dac_2a")
        result = oecd_dac_2a_raw_file(
            context=dg.build_asset_context(), raw_file_store=file_store
        )

    bulk_download.assert_called_once_with(
        save_to_path=str(raw_dir), use_raw_cache=False
    )

    assert list_parquet_columns(Path(str(raw_dir)) / "data.parquet") == [
        "RECIPIENT",
        "Recipient",
        "DONOR",
        "Donor",
        "PART",
        "Part",
        "AIDTYPE",
        "Aid type",
        "DATATYPE",
        "Amount type",
        "TIME",
        "Year",
        "Value",
        "Flags",
    ]

    assert isinstance(result, dg.MaterializeResult)
    assert result.value == str(raw_dir / "data.parquet")
    assert result.metadata == {
        "dagster/uri": dg.TextMetadataValue(str(raw_dir / "data.parquet")),
        "file_size": dg.IntMetadataValue(10120),
    }
    [check] = result.check_results
    assert check.check_name == "file_not_empty"
    assert check.passed


def test_dac_2a_raw_file_check_fails_when_file_empty(tmp_path: Path) -> None:
    """Test DAC 2a raw file check fails when the download is empty"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))

    with patch(
        "dfo_pipelines.defs.oecd.dac_2a.assets.bulk_download_dac2a",
        side_effect=partial(write_download, FIXTURES / "empty.parquet"),
    ):
        result = oecd_dac_2a_raw_file(
            context=dg.build_asset_context(), raw_file_store=file_store
        )

    assert isinstance(result, dg.MaterializeResult)

    [check] = result.check_results

    assert check.check_name == "file_not_empty"
    assert not check.passed


def test_dac_2a_raw_table_loads_parquet(tmp_path: Path) -> None:
    """Test DAC 2a raw table asset loads the parquet file into the database"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    result = oecd_dac_2a_raw_table(
        duckdb=duckdb, oecd_dac_2a_raw_file=str(FIXTURES / "data.parquet")
    )

    assert row_count(duckdb, "oecd_dac_2a.raw_dac_2a") == 25
    assert column_list(duckdb, "oecd_dac_2a.raw_dac_2a") == [
        "recipient_code",
        "recipient_name",
        "donor_code",
        "donor_name",
        "part_code",
        "part_name",
        "AIDTYPE",
        "Aid type",
        "DATATYPE",
        "Amount type",
        "TIME",
        "Year",
        "Value",
        "Flags",
    ]
    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata == {
        "dagster/table_name": dg.TextMetadataValue("oecd_dac_2a.raw_dac_2a"),
        "dagster/row_count": dg.IntMetadataValue(25),
    }
    [check] = result.check_results
    assert check.check_name == "table_not_empty"
    assert check.passed


def test_dac_2a_raw_table_check_fails_when_table_empty(tmp_path: Path) -> None:
    """Test DAC 2a raw table check fails when the parquet file has no rows"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    result = oecd_dac_2a_raw_table(
        duckdb=duckdb, oecd_dac_2a_raw_file=str(FIXTURES / "empty.parquet")
    )

    assert row_count(duckdb, "oecd_dac_2a.raw_dac_2a") == 0

    assert isinstance(result, dg.MaterializeResult)

    [check] = result.check_results

    assert check.check_name == "table_not_empty"
    assert not check.passed


def test_dac_2a_raw_table_overwrites_existing_table(tmp_path: Path) -> None:
    """Test DAC 2a raw table asset replaces the table rather than appending"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    oecd_dac_2a_raw_table(
        duckdb=duckdb, oecd_dac_2a_raw_file=str(FIXTURES / "data.parquet")
    )

    assert row_count(duckdb, "oecd_dac_2a.raw_dac_2a") == 25

    oecd_dac_2a_raw_table(
        duckdb=duckdb, oecd_dac_2a_raw_file=str(FIXTURES / "data-5.parquet")
    )

    assert row_count(duckdb, "oecd_dac_2a.raw_dac_2a") == 5


def test_dac_2a_raw_file_path_reaches_raw_table(tmp_path: Path) -> None:
    """Test the raw file asset's output path is injected into the raw table asset"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    with patch(
        "dfo_pipelines.defs.oecd.dac_2a.assets.bulk_download_dac2a",
        side_effect=partial(write_download, FIXTURES / "data.parquet"),
    ):
        result = dg.materialize(
            [oecd_dac_2a_raw_file, oecd_dac_2a_raw_table],
            resources={"raw_file_store": file_store, "duckdb": duckdb},
        )

    assert result.success

    checks = {e.check_name: e.passed for e in result.get_asset_check_evaluations()}

    assert checks == {"file_not_empty": True, "table_not_empty": True}
    assert row_count(duckdb, "oecd_dac_2a.raw_dac_2a") == 25
