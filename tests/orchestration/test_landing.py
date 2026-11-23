import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, tzinfo
from pathlib import Path
from unittest.mock import patch

import dagster as dg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from upath import UPath

from dfo_pipelines.extraction.metadata import METADATA_FILE_NAME, FetchedRelease
from dfo_pipelines.orchestration.automation import EAGER_WHEN_CHECKS_PASS
from dfo_pipelines.orchestration.landing import build_landing_asset
from dfo_pipelines.orchestration.raw_tables import RawTableSpec, build_raw_table_assets
from dfo_pipelines.orchestration.resources import UPathFileStoreResource
from tests.helpers import AutomationHarness, logged_messages

# Late enough in the UTC day that any zone east of UTC has moved to the next date.
DOWNLOADED_AT = datetime(2026, 8, 3, 23, 47, 5, tzinfo=UTC)

RELEASE_DATE = date(2026, 7, 7)


def fetch_rows(
    rows: int,
    *,
    release_dates: dict[str, date] | None = None,
    metadata: dict[str, dg.MetadataValue] | None = None,
):
    """A fetch hook writing a parquet file of `rows` rows to the path it is given"""

    def fetch(file_path: UPath):
        pq.write_table(pa.table({"code": list(range(rows))}), file_path)
        return FetchedRelease(
            release_dates=release_dates or {"source": RELEASE_DATE},
            metadata=metadata or {},
        )

    return fetch


def build_example(fetch, **kwargs) -> dg.AssetsDefinition:
    return build_landing_asset(
        group="example", dataset="things", title="Example", fetch=fetch, **kwargs
    )


@contextmanager
def patch_download_clock() -> Iterator[None]:
    """Freeze the instant the landing asset stamps a download with, in the zone it
    names, which a local-time reading would not"""

    def now(tz: tzinfo) -> datetime:
        return DOWNLOADED_AT.astimezone(tz)

    with patch("dfo_pipelines.orchestration.landing.datetime") as mock_datetime:
        mock_datetime.now.side_effect = now
        yield


def materialize_example(
    asset: dg.AssetsDefinition,
    tmp_path: Path,
    *,
    raise_on_error: bool = True,
) -> dg.ExecuteInProcessResult:
    """Materialize the asset against a file store under `tmp_path`"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "landing"))
    with patch_download_clock():
        return dg.materialize(
            [asset],
            resources={
                "landing_file_store": file_store,
                "io_manager": dg.mem_io_manager,
            },
            raise_on_error=raise_on_error,
        )


def materialization(result: dg.ExecuteInProcessResult) -> dg.AssetMaterialization:
    [event] = result.asset_materializations_for_node("example_landing")
    return event


def test_build_landing_asset_writes_the_download_and_its_metadata(
    tmp_path: Path,
) -> None:
    """Test the hook's file and the extraction metadata land in the directory of the
    download's UTC date, with the standard metadata and the hook's own"""
    asset = build_example(
        fetch_rows(3, metadata={"version": dg.MetadataValue.text("v20260801")})
    )

    result = materialize_example(asset, tmp_path)

    directory = tmp_path / "landing" / "example" / "things" / "2026" / "08" / "03"
    file_path = directory / "things.parquet"
    assert sorted(path.name for path in directory.iterdir()) == [
        METADATA_FILE_NAME,
        "things.parquet",
    ]
    assert json.loads((directory / METADATA_FILE_NAME).read_text()) == {
        "downloaded_at": "2026-08-03T23:47:05+00:00",
        "source_release_date": "2026-07-07",
    }
    assert result.output_for_node("example_landing") == str(directory)
    assert materialization(result).metadata == {
        "dagster/uri": dg.TextMetadataValue(str(directory)),
        "file_size": dg.IntMetadataValue(file_path.stat().st_size),
        "dagster/row_count": dg.IntMetadataValue(3),
        "version": dg.TextMetadataValue("v20260801"),
        "source_release_date": dg.TextMetadataValue("2026-07-07"),
    }
    [check] = result.get_asset_check_evaluations()
    assert check.check_name == "file_not_empty"
    assert check.passed


def test_build_landing_asset_records_each_release(tmp_path: Path) -> None:
    """Test a hook naming two releases records a date for each, on disk and in the
    materialization metadata"""
    asset = build_example(
        fetch_rows(
            1,
            release_dates={
                "crs": date(2026, 7, 1),
                "multisystem": date(2026, 6, 15),
            },
        )
    )

    result = materialize_example(asset, tmp_path)

    directory = tmp_path / "landing" / "example" / "things" / "2026" / "08" / "03"
    recorded = json.loads((directory / METADATA_FILE_NAME).read_text())
    assert recorded == {
        "downloaded_at": "2026-08-03T23:47:05+00:00",
        "crs_release_date": "2026-07-01",
        "multisystem_release_date": "2026-06-15",
    }
    metadata = materialization(result).metadata
    assert (
        metadata.items()
        >= {
            "crs_release_date": dg.TextMetadataValue("2026-07-01"),
            "multisystem_release_date": dg.TextMetadataValue("2026-06-15"),
        }.items()
    )
    assert "source_release_date" not in metadata


def test_build_landing_asset_check_fails_when_the_file_holds_no_rows(
    tmp_path: Path,
) -> None:
    """Test an empty download, which still carries a schema, fails the check"""
    result = materialize_example(
        build_example(fetch_rows(0)), tmp_path, raise_on_error=False
    )

    [check] = result.get_asset_check_evaluations()
    assert check.check_name == "file_not_empty"
    assert not check.passed


def test_build_landing_asset_writes_no_metadata_when_the_fetch_fails(
    tmp_path: Path,
) -> None:
    """Test a failed fetch leaves no record claiming a download happened"""

    def fail(file_path: UPath):
        raise RuntimeError("download refused")

    with pytest.raises(RuntimeError, match="download refused"):
        materialize_example(build_example(fail), tmp_path)

    assert not list((tmp_path / "landing").rglob(METADATA_FILE_NAME))


def test_build_landing_asset_writes_no_metadata_when_the_file_is_unreadable(
    tmp_path: Path,
) -> None:
    """Test a hook writing a truncated file leaves no record claiming a download"""

    def truncate(file_path: UPath):
        file_path.write_bytes(b"PAR1")
        return FetchedRelease(release_dates={"source": RELEASE_DATE}, metadata={})

    with pytest.raises(pa.ArrowInvalid):
        materialize_example(build_example(truncate), tmp_path)

    assert not list((tmp_path / "landing").rglob(METADATA_FILE_NAME))


def test_build_landing_asset_logs_where_it_wrote(tmp_path: Path) -> None:
    """Test the run log names the source and the file written"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "landing"))

    with patch_download_clock():
        messages = logged_messages(
            [build_example(fetch_rows(1))], {"landing_file_store": file_store}
        )

    directory = tmp_path / "landing" / "example" / "things" / "2026" / "08" / "03"
    assert f"Wrote Example data to {directory / 'things.parquet'}" in messages


def test_build_landing_asset_defines_the_asset_downstream_of_its_source() -> None:
    """Test the asset's key, dep, group, kinds, automation and blocking check"""
    asset = build_example(fetch_rows(1))

    key = dg.AssetKey("example_landing")
    assert asset.key == key
    spec = asset.specs_by_key[key]
    assert [dep.asset_key for dep in spec.deps] == [dg.AssetKey("example_source")]
    assert spec.group_name == "example"
    assert spec.kinds == {"file", "parquet"}
    assert spec.automation_condition == EAGER_WHEN_CHECKS_PASS
    assert spec.description == "Example download and its metadata"
    [check_spec] = asset.check_specs
    assert check_spec.name == "file_not_empty"
    assert check_spec.asset_key == key
    assert check_spec.blocking
    assert asset.op.retry_policy is None


def test_build_landing_asset_takes_a_retry_policy() -> None:
    """Test a given retry policy reaches the asset"""
    retry_policy = dg.RetryPolicy(max_retries=1)

    asset = build_example(fetch_rows(1), retry_policy=retry_policy)

    assert asset.op.retry_policy == retry_policy


class _ExampleAutomation(AutomationHarness):
    """Drives a stub source, the built landing asset and the raw tables loading it

    Assign to `version` to make the next observation see a republished source.
    """

    def __init__(self, tmp_path: Path) -> None:
        self.version = "v20260801"

        @dg.observable_source_asset(name="example_source", group_name="example")
        def example_source() -> dg.ObserveResult:
            return dg.ObserveResult(data_version=dg.DataVersion(self.version))

        self.landing = build_example(fetch_rows(1))
        self.raw_tables = build_raw_table_assets(
            group="example",
            title="Example",
            tables=[
                RawTableSpec(
                    asset="example_raw_table", file_stem="things", table="raw_things"
                )
            ],
            metadata_table="raw_things_metadata",
        )
        super().__init__(
            tmp_path,
            source=example_source,
            raw_assets=[self.landing, self.raw_tables],
        )

    def _start_mocks(self) -> None:
        """Nothing to patch. The stub source and fetch hook reach no network"""


def test_build_landing_asset_is_not_rerun_when_the_source_is_unchanged(
    tmp_path: Path,
) -> None:
    """Test a repeat observation at an unchanged data version requests no rerun"""
    with _ExampleAutomation(tmp_path) as automation:
        automation.observe()
        automation.materialize(automation.landing, automation.raw_tables)
        # A baseline tick, not evidence. Every condition is reset on its initial
        # evaluation, so the first tick requests nothing whatever the source did.
        automation.tick()

        automation.observe()

        assert set(automation.tick().values()) == {0}


def test_build_landing_asset_is_rerun_with_its_raw_tables_when_the_source_changes(
    tmp_path: Path,
) -> None:
    """Test a changed data version requests the landing asset and the tables loading
    it on one tick, and only once. The tables load the directory the landing
    asset returned"""
    with _ExampleAutomation(tmp_path) as automation:
        automation.observe()
        automation.materialize(automation.landing, automation.raw_tables)
        automation.tick()  # baseline, as above

        automation.version = "v20260901"
        automation.observe()

        # eager() counts a dependency that will be requested as updated, so the
        # chain is launched in one run.
        assert set(automation.tick().values()) == {1}

        automation.materialize(automation.landing, automation.raw_tables)

        assert set(automation.tick().values()) == {0}
