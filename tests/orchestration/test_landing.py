import json
from collections.abc import Iterator, Mapping
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
from dfo_pipelines.orchestration.landing import Observation, build_landing_asset
from dfo_pipelines.orchestration.raw_tables import RawTableSpec, build_raw_table_assets
from dfo_pipelines.orchestration.resources import UPathFileStoreResource
from tests.helpers import (
    AutomationHarness,
    check_outcomes,
    logged_messages,
    materialization,
    materialize_landing,
    record_observation,
)

# Late enough in the UTC day that any zone east of UTC has moved to the next date.
DOWNLOADED_AT = datetime(2026, 8, 3, 23, 47, 5, tzinfo=UTC)

RELEASE_DATE = date(2026, 7, 7)


def parquet_with_rows(rows: int) -> bytes:
    """A parquet file of `rows` rows"""
    sink = pa.BufferOutputStream()
    pq.write_table(pa.table({"code": list(range(rows))}), sink)
    return sink.getvalue().to_pybytes()


def fetch_files(
    contents: Mapping[str, bytes],
    *,
    release_dates: dict[str, date] | None = None,
    metadata: dict[str, dg.MetadataValue] | None = None,
):
    """A fetch hook writing each file in `contents` to the directory it is given"""

    def fetch(directory: UPath, observation: Observation):
        for name, content in contents.items():
            (directory / name).write_bytes(content)
        return FetchedRelease(
            release_dates={"source": RELEASE_DATE}
            if release_dates is None
            else release_dates,
            metadata=metadata or {},
        )

    return fetch


def fetch_rows(rows: int, **kwargs):
    """A fetch hook writing `things.parquet` with `rows` rows"""
    return fetch_files({"things.parquet": parquet_with_rows(rows)}, **kwargs)


@dg.observable_source_asset(name="example_source", group_name="example")
def example_source() -> dg.ObserveResult:
    return dg.ObserveResult(data_version=dg.DataVersion("v20260801"))


@pytest.fixture
def instance() -> Iterator[dg.DagsterInstance]:
    """An instance holding an observation of the example source"""
    with dg.DagsterInstance.ephemeral() as instance:
        record_observation(
            instance, example_source, data_version="v20260801", metadata={}
        )
        yield instance


def build_example(
    fetch,
    *,
    files: tuple[str, ...] = ("things.parquet",),
    source: dg.SourceAsset = example_source,
    **kwargs,
) -> dg.AssetsDefinition:
    return build_landing_asset(
        group="example",
        source=source,
        dataset="things",
        title="Example",
        files=files,
        fetch=fetch,
        **kwargs,
    )


@pytest.fixture(autouse=True)
def frozen_download_clock() -> Iterator[None]:
    """Freeze the instant the landing asset stamps a download with, in the zone it
    names, which a local-time reading would not"""

    def now(tz: tzinfo) -> datetime:
        return DOWNLOADED_AT.astimezone(tz)

    with patch("dfo_pipelines.orchestration.landing.datetime") as mock_datetime:
        mock_datetime.now.side_effect = now
        yield


def test_build_landing_asset_writes_the_download_and_its_metadata(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """Test the hook's file and the extraction metadata land in the directory of the
    download's UTC date, with the standard metadata and the hook's own"""
    asset = build_example(
        fetch_rows(3, metadata={"version": dg.MetadataValue.text("v20260801")})
    )

    result = materialize_landing(asset, tmp_path, instance=instance)

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
    assert check_outcomes(result) == {"things_not_empty": True}


def test_build_landing_asset_records_each_release(
    tmp_path: Path, instance: dg.DagsterInstance
) -> None:
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

    result = materialize_landing(asset, tmp_path, instance=instance)

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
    instance: dg.DagsterInstance,
) -> None:
    """Test an empty download, which still carries a schema, fails the check"""
    result = materialize_landing(
        build_example(fetch_rows(0)), tmp_path, raise_on_error=False, instance=instance
    )

    assert check_outcomes(result) == {"things_not_empty": False}


def test_build_landing_asset_writes_no_metadata_when_the_fetch_fails(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """Test a failed fetch leaves no record claiming a download happened"""

    def fail(directory: UPath, observation: Observation):
        raise RuntimeError("download refused")

    with pytest.raises(RuntimeError, match="download refused"):
        materialize_landing(build_example(fail), tmp_path, instance=instance)

    assert not list((tmp_path / "landing").rglob(METADATA_FILE_NAME))


def test_build_landing_asset_writes_no_metadata_when_the_file_is_unreadable(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """Test a hook writing a truncated file leaves no record claiming a download"""

    with pytest.raises(pa.ArrowInvalid):
        materialize_landing(
            build_example(fetch_files({"things.parquet": b"PAR1"})),
            tmp_path,
            instance=instance,
        )

    assert not list((tmp_path / "landing").rglob(METADATA_FILE_NAME))


@pytest.mark.parametrize(
    ("file", "content", "passed"),
    [
        ("things.csv", b"code\n1\n", True),
        ("things.csv", b"code\n", False),
        ("things.mcf", b"Node: dcid:thing\ntypeOf: dcs:Thing\n", True),
        ("things.mcf", b"# Compiled from the bench\n", False),
        ("things.txt", b"x", True),
        ("things.txt", b"", False),
    ],
)
def test_build_landing_asset_checks_a_file_by_its_type(
    tmp_path: Path,
    instance: dg.DagsterInstance,
    file: str,
    content: bytes,
    passed: bool,
) -> None:
    """Test a CSV file passes with a row after its header, an MCF file with a node,
    and any other file with a byte"""
    asset = build_example(fetch_files({file: content}), files=(file,))

    result = materialize_landing(
        asset, tmp_path, raise_on_error=False, instance=instance
    )

    assert check_outcomes(result) == {"things_not_empty": passed}


def test_build_landing_asset_counts_the_rows_of_each_table(
    tmp_path: Path, instance: dg.DagsterInstance
) -> None:
    """Test several files record their total size, and the rows of each parquet and
    CSV file and the nodes of each MCF file with their total. A quoted line break
    does not split a CSV row"""
    files = {
        "first.parquet": parquet_with_rows(2),
        "second.csv": b'code,note\n1,"two\nlines"\n2,x\n3,y\n',
        "third.mcf": b"Node: dcid:first\n\nNode: dcid:second\n",
        "notes.txt": b"notes",
    }
    asset = build_example(fetch_files(files), files=tuple(files))

    result = materialize_landing(asset, tmp_path, instance=instance)

    directory = tmp_path / "landing" / "example" / "things" / "2026" / "08" / "03"
    assert materialization(result).metadata == {
        "dagster/uri": dg.TextMetadataValue(str(directory)),
        "file_size": dg.IntMetadataValue(
            sum(len(content) for content in files.values())
        ),
        "dagster/row_count": dg.IntMetadataValue(7),
        "first_row_count": dg.IntMetadataValue(2),
        "second_row_count": dg.IntMetadataValue(3),
        "third_row_count": dg.IntMetadataValue(2),
        "source_release_date": dg.TextMetadataValue("2026-07-07"),
    }
    assert check_outcomes(result) == {
        "first_not_empty": True,
        "second_not_empty": True,
        "third_not_empty": True,
        "notes_not_empty": True,
    }


def test_build_landing_asset_records_no_size_or_rows_without_declared_files(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """Test a source declaring no files records neither a file size nor a row count"""
    asset = build_example(
        fetch_files({"things.parquet": parquet_with_rows(1)}),
        files=(),
        checks={"ready": lambda directory: True},
    )

    result = materialize_landing(asset, tmp_path, instance=instance)

    directory = tmp_path / "landing" / "example" / "things" / "2026" / "08" / "03"
    assert materialization(result).metadata == {
        "dagster/uri": dg.TextMetadataValue(str(directory)),
        "source_release_date": dg.TextMetadataValue("2026-07-07"),
    }


def test_build_landing_asset_runs_each_named_check_on_the_directory(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """Test a named check is evaluated on the landing directory, beside the file
    checks, and fails when its predicate is false"""
    asset = build_example(
        fetch_rows(1),
        checks={
            "things_landed": lambda directory: (directory / "things.parquet").exists(),
            "never": lambda directory: False,
        },
    )

    result = materialize_landing(
        asset, tmp_path, raise_on_error=False, instance=instance
    )

    assert check_outcomes(result) == {
        "things_not_empty": True,
        "things_landed": True,
        "never": False,
    }


def test_build_landing_asset_fails_when_the_fetch_records_no_release(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """Test a fetch returning no release date fails the run and records nothing"""
    with pytest.raises(ValueError, match="must name at least one source"):
        materialize_landing(
            build_example(fetch_rows(1, release_dates={})), tmp_path, instance=instance
        )

    assert not list((tmp_path / "landing").rglob(METADATA_FILE_NAME))


def test_build_landing_asset_fails_when_a_declared_file_is_missing(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """Test a declared file the fetch did not write fails the run and records
    nothing"""
    asset = build_example(fetch_rows(1), files=("things.parquet", "more.csv"))

    with pytest.raises(FileNotFoundError, match=r"more\.csv"):
        materialize_landing(asset, tmp_path, instance=instance)

    assert not list((tmp_path / "landing").rglob(METADATA_FILE_NAME))


def test_build_landing_asset_gives_the_fetch_the_latest_observation(
    tmp_path: Path,
) -> None:
    """Test the fetch is given the source's latest observation, with each metadata
    entry's plain value"""
    observations: list[Observation] = []

    def fetch(directory: UPath, observation: Observation):
        observations.append(observation)
        return fetch_rows(1)(directory, observation)

    instance = dg.DagsterInstance.ephemeral()
    record_observation(
        instance,
        example_source,
        data_version="v20260701",
        metadata={"version": dg.MetadataValue.text("v20260701")},
    )
    record_observation(
        instance,
        example_source,
        data_version="v20260801",
        metadata={"version": dg.MetadataValue.text("v20260801"), "files": 2},
    )

    materialize_landing(build_example(fetch), tmp_path, instance=instance)

    assert observations == [
        Observation(
            data_version="v20260801", metadata={"version": "v20260801", "files": 2}
        )
    ]


def test_build_landing_asset_fails_before_its_source_is_observed(
    tmp_path: Path,
) -> None:
    """Test the asset refuses to fetch while its source has no observation, and
    writes nothing"""
    with pytest.raises(
        ValueError, match="example_source has no observation; observe it first"
    ):
        materialize_landing(build_example(fetch_rows(1)), tmp_path)

    assert not (tmp_path / "landing").exists()


def test_build_landing_asset_logs_where_it_wrote(
    tmp_path: Path, instance: dg.DagsterInstance
) -> None:
    """Test the run log names the source and the directory written"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "landing"))

    messages = logged_messages(
        [build_example(fetch_rows(1))],
        {"landing_file_store": file_store},
        instance=instance,
    )

    directory = tmp_path / "landing" / "example" / "things" / "2026" / "08" / "03"
    assert f"Wrote Example data to {directory}" in messages


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
    assert check_spec.name == "things_not_empty"
    assert check_spec.asset_key == key
    assert check_spec.blocking
    assert asset.op.retry_policy is None


def test_build_landing_asset_declares_a_blocking_check_per_file_and_named_check() -> (
    None
):
    """Test each declared file adds its kind and a `<stem>_not_empty` check, and
    each named check its own, all blocking"""
    asset = build_example(
        fetch_rows(1),
        files=("first.parquet", "second.csv", "notes.txt"),
        checks={"ready": lambda directory: True},
    )

    assert asset.specs_by_key[asset.key].kinds == {"file", "parquet", "csv"}
    assert {spec.name: spec.blocking for spec in asset.check_specs} == {
        "first_not_empty": True,
        "second_not_empty": True,
        "notes_not_empty": True,
        "ready": True,
    }


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
        def versioned_source() -> dg.ObserveResult:
            return dg.ObserveResult(data_version=dg.DataVersion(self.version))

        self.landing = build_example(fetch_rows(1), source=versioned_source)
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
            source=versioned_source,
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
