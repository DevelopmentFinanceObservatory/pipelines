import csv
import json
import re
import subprocess
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import dagster as dg
import pytest

from dfo_pipelines.defs.dfo.ontology.assets import (
    BENCH,
    CODE_MAPPING_CSV,
    COMPILE_REPORT,
    IMPORT_NAME,
    ONTOLOGY_MCF,
    dfo_ontology_dc_export,
    dfo_ontology_landing,
    dfo_ontology_raw_code_mapping_table,
    dfo_ontology_source,
)
from dfo_pipelines.extraction.metadata import METADATA_FILE_NAME
from dfo_pipelines.orchestration.landing import Observation
from dfo_pipelines.resources.file_store import UPathFileStoreResource
from dfo_pipelines.resources.warehouse import DuckDBWarehouseResource
from tests.helpers import (
    exported_directory,
    landed_directory,
    materialize_landing,
    meta,
    record_observation,
)

FIXTURES = Path(__file__).parent / "fixtures"

STUB_MCF = """# bench: prod (https://developmentfinance.wikibase.cloud)

Node: dcid:FinancialFlow
typeOf: dcid:Class
name: "Financial flow"

Node: dcid:crsPurpose
typeOf: dcid:Property
name: "CRS purpose"
"""

STUB_REPORT = "G1 identity: 2 nodes carry no dcid\n"


def mapping_rows(path: Path = FIXTURES / CODE_MAPPING_CSV) -> list[dict[str, str]]:
    """The rows of a code mapping, read independently of the asset under test"""
    with path.open() as handle:
        return list(csv.DictReader(handle))


def write_mapping(directory: Path, rows: list[dict[str, str]]) -> Path:
    """Write a code mapping holding the given rows"""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / CODE_MAPPING_CSV
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=mapping_rows()[0].keys(), lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)
    return path


def fake_compile(
    *,
    returncode: int = 0,
    mcf: str = STUB_MCF,
    rows: list[dict[str, str]] | None = None,
    omit: tuple[str, ...] = (),
    report: str = STUB_REPORT,
) -> Callable[..., subprocess.CompletedProcess[str]]:
    """Stand in for the compiler, writing what it writes where it writes it

    The report is written whether the compile passes or fails, as the compiler
    does, because the asset keeps it either way.
    """

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        output = Path(command[command.index("--output") + 1])
        (output.parent / COMPILE_REPORT).write_text(report)
        if returncode == 0:
            if ONTOLOGY_MCF not in omit:
                output.write_text(mcf)
            if CODE_MAPPING_CSV not in omit:
                write_mapping(output.parent, mapping_rows() if rows is None else rows)
        return subprocess.CompletedProcess(
            command,
            returncode,
            stdout="",
            stderr="" if returncode == 0 else report,
        )

    return run


@pytest.fixture
def compiler() -> Iterator[Any]:
    """The ontology compiler, standing in for the console script

    Compiles successfully. A test that needs another outcome reassigns
    `side_effect` to a differently configured `fake_compile`.
    """
    with patch("dfo_pipelines.orchestration.subprocesses.subprocess.run") as mock:
        mock.side_effect = fake_compile()
        yield mock


def observe() -> Observation:
    """The source's observation of the instance, as the landing asset reads it"""
    observe_fn = dfo_ontology_source.observe_fn
    assert observe_fn is not None
    result = observe_fn()
    assert isinstance(result, dg.ObserveResult)
    assert result.data_version is not None
    assert result.metadata is not None
    metadata = {}
    for key, value in result.metadata.items():
        assert isinstance(value, dg.MetadataValue)
        metadata[key] = value.value
    return Observation(data_version=result.data_version.value, metadata=metadata)


def observed_instance() -> dg.DagsterInstance:
    """An instance holding the source's observation, as the daily tick records it"""
    instance = dg.DagsterInstance.ephemeral()
    observation = observe()
    record_observation(
        instance,
        dfo_ontology_source,
        data_version=observation.data_version,
        metadata=observation.metadata,
    )
    return instance


def materialize_observed(
    tmp_path: Path,
    compiler: Any,
    *,
    landing_compile: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    raise_on_error: bool = True,
) -> dg.ExecuteInProcessResult:
    """Observe the source, then materialize the landing asset, compiling with
    `landing_compile` from the observation on when given"""
    instance = observed_instance()
    if landing_compile is not None:
        compiler.side_effect = landing_compile
    return materialize_landing(
        dfo_ontology_landing,
        tmp_path,
        instance=instance,
        raise_on_error=raise_on_error,
    )


def checks(result: dg.MaterializeResult) -> dict[str, bool]:
    """The asset's checks, keyed on name"""
    return {str(check.check_name): bool(check.passed) for check in result.check_results}


def test_landing_raises_when_the_compiler_fails(tmp_path: Path, compiler: Any) -> None:
    """Test a bench the compiler rejects fails the asset, carrying its report

    The compiler writes its account of what is wrong to stderr, which is all a
    failed run has to explain itself with.
    """
    with pytest.raises(RuntimeError, match=STUB_REPORT.strip()):
        materialize_observed(
            tmp_path, compiler, landing_compile=fake_compile(returncode=1)
        )


@pytest.mark.usefixtures("download_clock")
def test_landing_publishes_nothing_when_an_output_is_missing(
    tmp_path: Path, compiler: Any
) -> None:
    """Test a compile missing an output leaves what an earlier one published"""
    published = landed_directory(materialize_observed(tmp_path, compiler))

    with pytest.raises(RuntimeError, match=re.escape(CODE_MAPPING_CSV)):
        materialize_observed(
            tmp_path,
            compiler,
            landing_compile=fake_compile(
                mcf="Node: dcid:Replaced\n", omit=(CODE_MAPPING_CSV,)
            ),
        )

    assert (published / ONTOLOGY_MCF).read_text() == STUB_MCF
    assert list(published.parent.iterdir()) == [published]


def test_source_versions_the_compile(compiler: Any) -> None:
    """Test the source reports the bench it compiled and versions the compile,
    the same each time the ontology compiles the same"""
    first, second = observe(), observe()

    assert first == second
    assert first.metadata == {
        "dagster/uri": BENCH.url,
        "version": first.data_version,
    }


@pytest.mark.parametrize(
    ("changed_compile", "changes_version"),
    [
        (fake_compile(mcf=STUB_MCF + "\nNode: dcid:Added\n"), True),
        (fake_compile(rows=mapping_rows()[:1]), True),
        (fake_compile(report="G1 identity: every node carries a dcid\n"), False),
    ],
    ids=["mcf", "code_mapping", "report"],
)
def test_source_version_follows_the_landed_files(
    compiler: Any,
    changed_compile: Callable[..., subprocess.CompletedProcess[str]],
    changes_version: bool,
) -> None:
    """Test a change to the ontology or code mapping changes the version, and a
    change to the report alone, which is not checked or loaded, does not"""
    observed = observe()
    compiler.side_effect = changed_compile

    assert (observe().data_version != observed.data_version) is changes_version


def test_landing_records_the_day_it_compiled_as_the_release_date(
    tmp_path: Path, compiler: Any
) -> None:
    """Test the ontology was released on the day of its compile, since the
    Wikibase instance publishes no releases of its own"""
    landed = landed_directory(materialize_observed(tmp_path, compiler))

    recorded = json.loads((landed / METADATA_FILE_NAME).read_text())
    assert recorded["downloaded_at"].startswith(recorded["source_release_date"])


@pytest.mark.usefixtures("download_clock")
def test_landing_raises_when_the_compile_differs_from_the_observed_one(
    tmp_path: Path, compiler: Any
) -> None:
    """Test a compile is refused when the instance changed after it was observed

    The change would otherwise be recorded under the observed version. What an
    earlier run landed stays.
    """
    published = landed_directory(materialize_observed(tmp_path, compiler))

    with pytest.raises(RuntimeError, match="changed after it was observed"):
        materialize_observed(
            tmp_path,
            compiler,
            landing_compile=fake_compile(mcf="Node: dcid:Replaced\n"),
        )

    assert (published / ONTOLOGY_MCF).read_text() == STUB_MCF
    assert list(published.parent.iterdir()) == [published]


def test_landing_check_fails_when_the_report_is_empty(
    tmp_path: Path, compiler: Any
) -> None:
    """Test the files check covers the report, which lands beside the ontology"""
    compiler.side_effect = fake_compile(report="")

    result = materialize_observed(tmp_path, compiler, raise_on_error=False)

    [evaluation] = result.get_asset_check_evaluations()
    assert evaluation.metadata["empty_files"] == dg.JsonMetadataValue([COMPILE_REPORT])


def test_raw_code_mapping_table_loads_every_mapping(tmp_path: Path) -> None:
    """Test the code mapping is loaded whole, for other pipelines to resolve against"""
    warehouse = DuckDBWarehouseResource(database=str(tmp_path / "database.duckdb"))
    rows = mapping_rows()

    result = dfo_ontology_raw_code_mapping_table(
        warehouse=warehouse,
        dfo_ontology_landing=str(FIXTURES),
    )

    loaded = warehouse.get_rows("dfo_ontology.raw_code_mapping")
    assert len(loaded) == len(rows)
    assert list(loaded[0]) == list(rows[0])
    assert isinstance(result, dg.MaterializeResult)
    assert meta(result, "dagster/row_count", dg.IntMetadataValue).value == len(rows)
    assert checks(result) == {"table_not_empty": True}


def test_raw_code_mapping_table_replaces_what_it_finds(tmp_path: Path) -> None:
    """Test a refreshed ontology replaces the table rather than appending to it"""
    warehouse = DuckDBWarehouseResource(database=str(tmp_path / "database.duckdb"))
    rows = mapping_rows()
    fewer = rows[: len(rows) // 2]
    write_mapping(tmp_path / "refreshed", fewer)

    dfo_ontology_raw_code_mapping_table(
        warehouse=warehouse,
        dfo_ontology_landing=str(FIXTURES),
    )
    assert len(warehouse.get_rows("dfo_ontology.raw_code_mapping")) == len(rows)

    dfo_ontology_raw_code_mapping_table(
        warehouse=warehouse,
        dfo_ontology_landing=str(tmp_path / "refreshed"),
    )

    assert len(warehouse.get_rows("dfo_ontology.raw_code_mapping")) == len(fewer)


def test_raw_code_mapping_table_check_fails_when_the_mapping_has_no_rows(
    tmp_path: Path,
) -> None:
    """Test the table check fails when the mapping carries only its header"""
    warehouse = DuckDBWarehouseResource(database=str(tmp_path / "database.duckdb"))
    write_mapping(tmp_path / "empty", [])

    result = dfo_ontology_raw_code_mapping_table(
        warehouse=warehouse,
        dfo_ontology_landing=str(tmp_path / "empty"),
    )

    assert isinstance(result, dg.MaterializeResult)
    assert checks(result) == {"table_not_empty": False}


def build_export(tmp_path: Path) -> tuple[dg.MaterializeResult, Path]:
    """Materialize the export asset against a compiled ontology on disk"""
    landing_directory = tmp_path / "landing"
    landing_directory.mkdir()
    (landing_directory / ONTOLOGY_MCF).write_text(STUB_MCF)
    export_file_store = UPathFileStoreResource(base_path=str(tmp_path / "processed"))

    result = dfo_ontology_dc_export(
        context=dg.build_asset_context(),
        export_file_store=export_file_store,
        dfo_ontology_landing=str(landing_directory),
    )
    assert isinstance(result, dg.MaterializeResult)
    return result, exported_directory(result)


def test_export_publishes_the_compiled_mcf_unchanged(tmp_path: Path) -> None:
    """Test the bundle carries the ontology exactly as the compiler emitted it

    This import publishes what it was given rather than building nodes from the
    warehouse, so the export must not rewrite what passes through it.
    """
    _, out_dir = build_export(tmp_path)

    assert (out_dir / ONTOLOGY_MCF).read_text() == STUB_MCF


def test_export_carries_the_files_the_import_needs(tmp_path: Path) -> None:
    """Test the bundle holds the ontology, its provenance and the config

    Any further MCF must be empty. dcp-tools opens a collection for custom
    nodes and writes it out whether or not anything was added to it, and this
    import declares its nodes in the compiled MCF alone.
    """
    _, out_dir = build_export(tmp_path)

    written = {path.name: path for path in out_dir.iterdir()}

    assert {"config.json", ONTOLOGY_MCF, "provenance.mcf"} <= set(written)
    for name, path in written.items():
        if name.endswith(".mcf") and name not in {ONTOLOGY_MCF, "provenance.mcf"}:
            assert path.read_text() == ""


def test_export_counts_the_nodes_it_publishes(tmp_path: Path) -> None:
    """Test the export reports the node count of the ontology it copied"""
    result, _ = build_export(tmp_path)

    node_count = meta(result, "node_count", dg.IntMetadataValue).value

    assert node_count == STUB_MCF.count("Node: ")


def test_export_removes_its_directory_when_the_copy_fails(tmp_path: Path) -> None:
    """Test an export that fails to copy the ontology, after writing the rest of
    the bundle, leaves no directory to ingest"""
    landing_directory = tmp_path / "landing"
    landing_directory.mkdir()
    export_directory = tmp_path / "processed"

    with pytest.raises(FileNotFoundError):
        dfo_ontology_dc_export(
            context=dg.build_asset_context(),
            export_file_store=UPathFileStoreResource(base_path=str(export_directory)),
            dfo_ontology_landing=str(landing_directory),
        )

    assert list((export_directory / IMPORT_NAME).iterdir()) == []


def test_the_compiled_ontology_reaches_the_table_and_the_bundle(
    tmp_path: Path, compiler: Any
) -> None:
    """Test the directory the compiler writes is what both consumers read"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "landing"))
    export_file_store = UPathFileStoreResource(base_path=str(tmp_path / "processed"))
    warehouse = DuckDBWarehouseResource(database=str(tmp_path / "database.duckdb"))

    result = dg.materialize(
        [
            dfo_ontology_landing,
            dfo_ontology_raw_code_mapping_table,
            dfo_ontology_dc_export,
        ],
        resources={
            "landing_file_store": file_store,
            "export_file_store": export_file_store,
            "warehouse": warehouse,
        },
        instance=observed_instance(),
    )

    assert result.success
    assert all(check.passed for check in result.get_asset_check_evaluations()), (
        result.get_asset_check_evaluations()
    )
    assert len(warehouse.get_rows("dfo_ontology.raw_code_mapping")) == len(
        mapping_rows()
    )
