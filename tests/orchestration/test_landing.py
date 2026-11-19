import hashlib
import json
import logging
import shutil
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

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
from tests.helpers import AutomationHarness, logged_messages, patch_download_clock

# Late enough in the UTC day that any zone east of UTC has moved to the next date.
DOWNLOADED_AT = datetime(2026, 8, 3, 23, 47, 5, tzinfo=UTC)

RELEASE_DATE = date(2026, 7, 7)


def fetch_rows(
    rows: int,
    *,
    release_dates: dict[str, date] | None = None,
    metadata: dict[str, str] | None = None,
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


def materialize_example(
    asset: dg.AssetsDefinition,
    tmp_path: Path,
    *,
    instance: dg.DagsterInstance | None = None,
    at: datetime = DOWNLOADED_AT,
    base: str = "landing",
    raise_on_error: bool = True,
) -> dg.ExecuteInProcessResult:
    """Materialize the asset at `at` against a file store at `tmp_path / base`"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / base))
    with patch_download_clock(at):
        return dg.materialize(
            [asset],
            resources={
                "landing_file_store": file_store,
                "io_manager": dg.mem_io_manager,
            },
            instance=instance or dg.DagsterInstance.ephemeral(),
            raise_on_error=raise_on_error,
        )


def materialization(result: dg.ExecuteInProcessResult) -> dg.AssetMaterialization:
    [event] = result.asset_materializations_for_node("example_landing")
    return event


def recorded(directory: Path) -> dict:
    return json.loads((directory / METADATA_FILE_NAME).read_text())


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


DATASET_DIRECTORY = Path("landing", "example", "things")
DIRECTORY = DATASET_DIRECTORY / "20260803T234705Z"
NEXT_MORNING = datetime(2026, 8, 4, 6, 0, 0, tzinfo=UTC)
NEXT_MORNING_DIRECTORY = DATASET_DIRECTORY / "20260804T060000Z"


def test_build_landing_asset_writes_the_download_and_its_metadata(
    tmp_path: Path,
) -> None:
    """Test the hook's file and the extraction metadata land in a directory named for
    the UTC download instant, with the standard metadata and the hook's own"""
    asset = build_example(fetch_rows(3, metadata={"version": "v20260801"}))

    result = materialize_example(asset, tmp_path)

    directory = tmp_path / DIRECTORY
    file_path = directory / "things.parquet"
    assert sorted(path.name for path in directory.iterdir()) == [
        METADATA_FILE_NAME,
        "things.parquet",
    ]
    assert recorded(directory) == {
        "downloaded_at": "2026-08-03T23:47:05+00:00",
        "run_id": result.run_id,
        "source_release_date": "2026-07-07",
        "version": "v20260801",
        "files": {"things.parquet": sha256(file_path)},
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

    assert (
        recorded(tmp_path / DIRECTORY).items()
        >= {
            "crs_release_date": "2026-07-01",
            "multisystem_release_date": "2026-06-15",
        }.items()
    )
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


def test_build_landing_asset_removes_the_directory_when_the_fetch_fails(
    tmp_path: Path,
) -> None:
    """Test a failed fetch leaves nothing behind, so no directory claims a download"""

    def fail(file_path: UPath):
        file_path.write_bytes(b"partial")
        raise RuntimeError("download refused")

    with pytest.raises(RuntimeError, match="download refused"):
        materialize_example(build_example(fail), tmp_path)

    assert not list((tmp_path / DATASET_DIRECTORY).iterdir())


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


def test_build_landing_asset_refuses_a_fetch_without_a_release_date(
    tmp_path: Path,
) -> None:
    """Test a hook naming no release leaves no record claiming a download"""

    def undated(file_path: UPath):
        pq.write_table(pa.table({"code": [1]}), file_path)
        return FetchedRelease(release_dates={}, metadata={})

    with pytest.raises(ValueError, match="The Example fetch returned no release dates"):
        materialize_example(build_example(undated), tmp_path)

    assert not list((tmp_path / "landing").rglob(METADATA_FILE_NAME))


def fail_to_remove_directories(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make removing a landing directory fail, with an error that is not an OSError"""

    def refuse(self: UPath, recursive: bool = True) -> None:
        raise ValueError(f"cannot remove {self}")

    monkeypatch.setattr(type(UPath("/")), "rmdir", refuse)


def user_messages(instance: dg.DagsterInstance) -> list[str]:
    """Every message the instance's runs logged"""
    return [
        entry.user_message
        for run in instance.get_runs()
        for entry in instance.all_logs(run.run_id)
    ]


def test_build_landing_asset_raises_the_fetch_error_when_cleanup_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test a directory that cannot be removed after a failed fetch is logged, and
    the fetch's error is the one raised"""

    def fail(file_path: UPath):
        raise RuntimeError("download refused")

    fail_to_remove_directories(monkeypatch)
    instance = dg.DagsterInstance.ephemeral()

    with pytest.raises(RuntimeError, match="download refused"):
        materialize_example(build_example(fail), tmp_path, instance=instance)

    directory = tmp_path / DIRECTORY
    assert f"Could not remove {directory}: cannot remove {directory}" in (
        user_messages(instance)
    )


def test_build_landing_asset_refuses_a_directory_that_exists(
    tmp_path: Path,
) -> None:
    """Test a second download stamped with the same instant leaves the first alone"""
    instance = dg.DagsterInstance.ephemeral()
    materialize_example(build_example(fetch_rows(1)), tmp_path, instance=instance)
    first = recorded(tmp_path / DIRECTORY)

    with pytest.raises(FileExistsError, match="20260803T234705Z"):
        materialize_example(build_example(fetch_rows(2)), tmp_path, instance=instance)

    assert recorded(tmp_path / DIRECTORY) == first


def test_build_landing_asset_keeps_the_earlier_directory_for_the_same_download(
    tmp_path: Path,
) -> None:
    """Test a download matching the latest materialization's files and release dates
    deletes its own directory and returns the earlier one, described as recorded"""
    instance = dg.DagsterInstance.ephemeral()
    asset = build_example(fetch_rows(1))
    first = materialize_example(asset, tmp_path, instance=instance)

    second = materialize_example(asset, tmp_path, instance=instance, at=NEXT_MORNING)

    directory = tmp_path / DIRECTORY
    assert second.output_for_node("example_landing") == str(directory)
    assert materialization(second).metadata["dagster/uri"] == dg.TextMetadataValue(
        str(directory)
    )
    assert list((tmp_path / DATASET_DIRECTORY).iterdir()) == [directory]
    assert recorded(directory)["run_id"] == first.run_id
    assert (
        f"Download matches {directory}, so {tmp_path / NEXT_MORNING_DIRECTORY} was deleted"
        in [entry.user_message for entry in instance.all_logs(second.run_id)]
    )


def test_build_landing_asset_reports_the_earlier_version_for_the_same_download(
    tmp_path: Path,
) -> None:
    """Test a duplicate download fetched under another version reports the version
    the returned directory recorded"""
    instance = dg.DagsterInstance.ephemeral()
    materialize_example(
        build_example(fetch_rows(1, metadata={"version": "v20260801"})),
        tmp_path,
        instance=instance,
    )

    second = materialize_example(
        build_example(fetch_rows(1, metadata={"version": "v20260804"})),
        tmp_path,
        instance=instance,
        at=NEXT_MORNING,
    )

    assert materialization(second).metadata["version"] == dg.TextMetadataValue(
        "v20260801"
    )


def test_build_landing_asset_returns_the_earlier_directory_when_cleanup_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test a duplicate directory that cannot be removed is logged, and the earlier
    directory is still returned"""
    instance = dg.DagsterInstance.ephemeral()
    asset = build_example(fetch_rows(1))
    materialize_example(asset, tmp_path, instance=instance)
    fail_to_remove_directories(monkeypatch)

    second = materialize_example(asset, tmp_path, instance=instance, at=NEXT_MORNING)

    directory = tmp_path / DIRECTORY
    duplicate = tmp_path / NEXT_MORNING_DIRECTORY
    assert second.output_for_node("example_landing") == str(directory)
    [warning] = [
        entry.user_message
        for entry in instance.all_logs(second.run_id)
        if entry.level == logging.WARNING
    ]
    assert warning == (
        f"Download matches {directory}, but {duplicate} could not be removed: "
        f"cannot remove {duplicate}"
    )


@pytest.mark.parametrize(
    "second_fetch",
    [
        pytest.param(fetch_rows(2), id="changed bytes"),
        pytest.param(
            fetch_rows(1, release_dates={"source": date(2026, 8, 1)}),
            id="changed release date",
        ),
    ],
)
def test_build_landing_asset_keeps_a_changed_download(
    tmp_path: Path, second_fetch
) -> None:
    """Test a download whose files or release dates differ from the latest
    materialization's is kept in its own directory"""
    instance = dg.DagsterInstance.ephemeral()
    materialize_example(build_example(fetch_rows(1)), tmp_path, instance=instance)

    second = materialize_example(
        build_example(second_fetch), tmp_path, instance=instance, at=NEXT_MORNING
    )

    assert second.output_for_node("example_landing") == str(
        tmp_path / NEXT_MORNING_DIRECTORY
    )
    assert recorded(tmp_path / NEXT_MORNING_DIRECTORY)["run_id"] == second.run_id
    assert recorded(tmp_path / DIRECTORY)


def _remove_directory(directory: Path) -> None:
    shutil.rmtree(directory)


def _remove_metadata(directory: Path) -> None:
    (directory / METADATA_FILE_NAME).unlink()


def _write_text_marker(directory: Path) -> None:
    (directory / METADATA_FILE_NAME).write_text("not JSON")


def _write_list_marker(directory: Path) -> None:
    (directory / METADATA_FILE_NAME).write_text('["files"]')


def _remove_file_hashes(directory: Path) -> None:
    """Rewrite the metadata without its file hashes"""
    metadata = recorded(directory)
    del metadata["files"]
    (directory / METADATA_FILE_NAME).write_text(json.dumps(metadata))


@pytest.mark.parametrize(
    "change_earlier",
    [
        pytest.param(_remove_directory, id="directory gone"),
        pytest.param(_remove_metadata, id="directory incomplete"),
        pytest.param(_remove_file_hashes, id="no file hashes"),
        pytest.param(_write_text_marker, id="metadata not JSON"),
        pytest.param(_write_list_marker, id="metadata not an object"),
    ],
)
def test_build_landing_asset_keeps_the_download_when_the_earlier_one_is_unusable(
    tmp_path: Path, change_earlier: Callable[[Path], None]
) -> None:
    """Test an earlier directory that cannot be compared is no duplicate, and the
    download is kept"""
    instance = dg.DagsterInstance.ephemeral()
    asset = build_example(fetch_rows(1))
    materialize_example(asset, tmp_path, instance=instance)
    change_earlier(tmp_path / DIRECTORY)

    second = materialize_example(asset, tmp_path, instance=instance, at=NEXT_MORNING)

    assert second.output_for_node("example_landing") == str(
        tmp_path / NEXT_MORNING_DIRECTORY
    )
    assert recorded(tmp_path / NEXT_MORNING_DIRECTORY)["run_id"] == second.run_id


def test_build_landing_asset_keeps_the_download_when_the_file_store_moved(
    tmp_path: Path,
) -> None:
    """Test a duplicate of a directory in another file store is kept, so the asset
    never returns a path outside the store it was given"""
    instance = dg.DagsterInstance.ephemeral()
    asset = build_example(fetch_rows(1))
    materialize_example(asset, tmp_path, instance=instance, base="old")

    second = materialize_example(asset, tmp_path, instance=instance, at=NEXT_MORNING)

    assert second.output_for_node("example_landing") == str(
        tmp_path / NEXT_MORNING_DIRECTORY
    )
    assert recorded(tmp_path / NEXT_MORNING_DIRECTORY)["run_id"] == second.run_id


def test_build_landing_asset_logs_where_it_wrote(tmp_path: Path) -> None:
    """Test the run log names the source and the file written"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "landing"))

    with patch_download_clock(DOWNLOADED_AT):
        messages = logged_messages(
            [build_example(fetch_rows(1))], {"landing_file_store": file_store}
        )

    assert f"Wrote Example data to {tmp_path / DIRECTORY / 'things.parquet'}" in (
        messages
    )


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
        # Each run stamps its own directory, which a shared second would refuse.
        with patch_download_clock(DOWNLOADED_AT):
            automation.materialize(automation.landing, automation.raw_tables)
        automation.tick()  # baseline, as above

        automation.version = "v20260901"
        automation.observe()

        # eager() counts a dependency that will be requested as updated, so the
        # chain is launched in one run.
        assert set(automation.tick().values()) == {1}

        with patch_download_clock(NEXT_MORNING):
            automation.materialize(automation.landing, automation.raw_tables)

        assert set(automation.tick().values()) == {0}
