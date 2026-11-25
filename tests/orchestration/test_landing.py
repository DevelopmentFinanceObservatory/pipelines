import json
from collections.abc import Iterator, Mapping
from datetime import date
from pathlib import Path

import dagster as dg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from upath import UPath

from dfo_pipelines.extraction.metadata import METADATA_FILE_NAME, FetchedRelease
from dfo_pipelines.orchestration.automation import EAGER_WHEN_CHECKS_PASS
from dfo_pipelines.orchestration.landing import Observation, build_landing_asset
from dfo_pipelines.orchestration.raw_tables import RawTableSpec, build_raw_table_assets
from dfo_pipelines.orchestration.resources.file_store import UPathFileStoreResource
from tests.helpers import (
    AutomationHarness,
    check_outcomes,
    logged_messages,
    materialization,
    materialize_landing,
    record_observation,
)

pytestmark = pytest.mark.usefixtures("download_clock")

RELEASE_DATE = date(2026, 7, 7)

DATASET_DIRECTORY = Path("landing", "example", "things")


def directory_of(tmp_path: Path, run_id: str) -> Path:
    """The directory a run landed its download in, from the `download_clock` instant"""
    return tmp_path / DATASET_DIRECTORY / f"20260803T234705Z_{run_id}"


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


def test_build_landing_asset_writes_the_download_and_its_metadata(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """Test the hook's file and the extraction metadata land in a directory named for
    the download's UTC instant, with the run, the standard metadata and the hook's
    own"""
    asset = build_example(
        fetch_rows(3, metadata={"version": dg.MetadataValue.text("v20260801")})
    )

    result = materialize_landing(asset, tmp_path, instance=instance)

    directory = directory_of(tmp_path, result.run_id)
    file_path = directory / "things.parquet"
    assert sorted(path.name for path in directory.iterdir()) == [
        METADATA_FILE_NAME,
        "things.parquet",
    ]
    assert json.loads((directory / METADATA_FILE_NAME).read_text()) == {
        "downloaded_at": "2026-08-03T23:47:05+00:00",
        "run_id": result.run_id,
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
    assert check_outcomes(result) == {"files_not_empty": True}


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

    directory = directory_of(tmp_path, result.run_id)
    recorded = json.loads((directory / METADATA_FILE_NAME).read_text())
    assert recorded == {
        "downloaded_at": "2026-08-03T23:47:05+00:00",
        "run_id": result.run_id,
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


def _fetch_partially_then_raise(error: BaseException):
    def fetch(directory: UPath, observation: Observation):
        (directory / "things.parquet").write_bytes(b"partial")
        raise error

    return fetch


def _unreadable(directory: UPath) -> bool:
    raise ValueError("check could not read the directory")


@pytest.mark.parametrize(
    ("landing", "error", "match"),
    [
        pytest.param(
            build_example(
                _fetch_partially_then_raise(RuntimeError("download refused"))
            ),
            RuntimeError,
            "download refused",
            id="fetch fails",
        ),
        pytest.param(
            build_example(fetch_files({"things.parquet": b"PAR1"})),
            pa.ArrowInvalid,
            None,
            id="file unreadable",
        ),
        pytest.param(
            build_example(fetch_rows(1, release_dates={})),
            ValueError,
            "must name at least one source",
            id="no release date",
        ),
        pytest.param(
            build_example(fetch_rows(1), files=("things.parquet", "more.csv")),
            FileNotFoundError,
            r"more\.csv",
            id="declared file missing",
        ),
        pytest.param(
            build_example(fetch_rows(1), checks={"readable": _unreadable}),
            ValueError,
            "check could not read the directory",
            id="check raises",
        ),
        pytest.param(
            build_example(_fetch_partially_then_raise(KeyboardInterrupt())),
            KeyboardInterrupt,
            None,
            id="keyboard interrupt",
        ),
        pytest.param(
            build_example(
                _fetch_partially_then_raise(dg.DagsterExecutionInterruptedError())
            ),
            dg.DagsterExecutionInterruptedError,
            None,
            id="run terminated",
        ),
    ],
)
def test_build_landing_asset_records_nothing_when_the_landing_fails(
    tmp_path: Path,
    instance: dg.DagsterInstance,
    landing: dg.AssetsDefinition,
    error: type[BaseException],
    match: str | None,
) -> None:
    """Test a landing that fails or is interrupted leaves no directory claiming a
    download happened"""
    with pytest.raises(error, match=match):
        materialize_landing(landing, tmp_path, instance=instance)

    assert not list((tmp_path / DATASET_DIRECTORY).iterdir())


def test_build_landing_asset_raises_the_fetch_error_when_removal_fails(
    tmp_path: Path, instance: dg.DagsterInstance, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test a directory that cannot be removed after a failed fetch is logged as a
    warning, and the fetch's error is the one raised"""

    def refuse(self: UPath, recursive: bool = True) -> None:
        raise ValueError(f"cannot remove {self}")

    monkeypatch.setattr(type(UPath(tmp_path)), "rmdir", refuse)

    with pytest.raises(RuntimeError, match="download refused"):
        materialize_landing(
            build_example(
                _fetch_partially_then_raise(RuntimeError("download refused"))
            ),
            tmp_path,
            instance=instance,
        )

    [run] = instance.get_runs()
    directory = directory_of(tmp_path, run.run_id)
    assert f"Could not remove {directory}: cannot remove {directory}" in [
        entry.user_message for entry in instance.all_logs(run.run_id)
    ]


def test_build_landing_asset_gives_runs_in_the_same_second_separate_directories(
    tmp_path: Path, instance: dg.DagsterInstance
) -> None:
    """Test two downloads stamped with the same instant each land in a directory of
    their own"""
    results = [
        materialize_landing(build_example(fetch_rows(1)), tmp_path, instance=instance)
        for _ in range(2)
    ]

    directories = [directory_of(tmp_path, result.run_id) for result in results]
    assert directories[0] != directories[1]
    assert all((directory / "things.parquet").exists() for directory in directories)


@pytest.mark.parametrize(
    ("file", "content", "passed"),
    [
        ("things.parquet", parquet_with_rows(1), True),
        ("things.parquet", parquet_with_rows(0), False),
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
    """Test a parquet file passes with a row, though an empty one still carries a
    schema, a CSV file with a row after its header, an MCF file with a node, and any
    other file with a byte"""
    asset = build_example(fetch_files({file: content}), files=(file,))

    result = materialize_landing(
        asset, tmp_path, raise_on_error=False, instance=instance
    )

    assert check_outcomes(result) == {"files_not_empty": passed}


def test_build_landing_asset_check_names_each_empty_file(
    tmp_path: Path, instance: dg.DagsterInstance
) -> None:
    """Test one empty file among several fails the check, which names only the empty
    ones"""
    files = {
        "first.parquet": parquet_with_rows(2),
        "second.csv": b"code\n",
        "third.mcf": b"# header only\n",
        "notes.txt": b"notes",
    }
    asset = build_example(fetch_files(files), files=tuple(files))

    result = materialize_landing(
        asset, tmp_path, instance=instance, raise_on_error=False
    )

    [evaluation] = result.get_asset_check_evaluations()
    assert evaluation.check_name == "files_not_empty"
    assert not evaluation.passed
    assert evaluation.metadata["empty_files"] == dg.JsonMetadataValue(
        ["second.csv", "third.mcf"]
    )


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

    directory = directory_of(tmp_path, result.run_id)
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

    directory = directory_of(tmp_path, result.run_id)
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
        "files_not_empty": True,
        "things_landed": True,
        "never": False,
    }


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

    [run] = instance.get_runs()
    directory = directory_of(tmp_path, run.run_id)
    assert f"Wrote Example data to {directory}" in messages


def test_build_landing_asset_defines_the_asset_downstream_of_its_source() -> None:
    """Test the asset's key, dep, group, kinds from its files, automation,
    description, and a blocking check for its files and each named check"""
    asset = build_example(
        fetch_rows(1),
        files=("first.parquet", "second.csv", "notes.txt"),
        checks={"ready": lambda directory: True},
    )

    key = dg.AssetKey("example_landing")
    assert asset.key == key
    spec = asset.specs_by_key[key]
    assert [dep.asset_key for dep in spec.deps] == [dg.AssetKey("example_source")]
    assert spec.group_name == "example"
    assert spec.kinds == {"file", "parquet", "csv"}
    assert spec.automation_condition == EAGER_WHEN_CHECKS_PASS
    assert spec.description == "Example download and its metadata"
    assert {
        (check_spec.name, check_spec.asset_key, check_spec.blocking)
        for check_spec in asset.check_specs
    } == {("files_not_empty", key, True), ("ready", key, True)}
    assert asset.op.retry_policy is None


def test_build_landing_asset_refuses_declared_files_sharing_a_stem() -> None:
    """Test two declared files with one stem are refused when the asset is defined,
    so neither goes uncounted"""
    with pytest.raises(ValueError, match=r"share the stems \['table'\]"):
        build_example(fetch_rows(1), files=("table.csv", "table.parquet"))


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
