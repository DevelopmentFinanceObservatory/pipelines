import csv
import dataclasses
import json
import re
import subprocess
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

import dagster as dg
import pytest
from dagster_duckdb import DuckDBResource
from upath import UPath

from dfo_pipelines.defs.dfo.ontology.assets import (
    BENCH,
    CODE_MAPPING_CSV,
    COMPILE_REPORT,
    ONTOLOGY_MCF,
    dfo_ontology_dc_export,
    dfo_ontology_landing,
    dfo_ontology_raw_code_mapping_table,
    dfo_ontology_source,
    fetch_ontology,
)
from dfo_pipelines.extraction.metadata import METADATA_FILE_NAME
from dfo_pipelines.extraction.wikibase import LatestRevision
from dfo_pipelines.orchestration.landing import Observation
from dfo_pipelines.orchestration.resources import UPathFileStoreResource
from tests.helpers import (
    column_list,
    landed_directory,
    materialization,
    materialize_landing,
    meta,
    record_observation,
    row_count,
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

LATEST_REVISION = LatestRevision(
    revision_id=2982, revised_at=datetime(2026, 11, 10, 13, 8, 34, tzinfo=UTC)
)


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
) -> Callable[..., subprocess.CompletedProcess[str]]:
    """Stand in for the compiler, writing what it writes where it writes it

    The report is written whether the compile passes or fails, as the compiler
    does, because the asset keeps it either way.
    """

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        output = Path(command[command.index("--output") + 1])
        (output.parent / COMPILE_REPORT).write_text(STUB_REPORT)
        if returncode == 0:
            if ONTOLOGY_MCF not in omit:
                output.write_text(mcf)
            if CODE_MAPPING_CSV not in omit:
                write_mapping(output.parent, mapping_rows() if rows is None else rows)
        return subprocess.CompletedProcess(
            command,
            returncode,
            stdout="",
            stderr="" if returncode == 0 else STUB_REPORT,
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


@pytest.fixture(autouse=True)
def wikibase() -> Iterator[Any]:
    """The Wikibase lookup, answering with `LATEST_REVISION` on every call

    A test that needs the instance edited reassigns `side_effect`.
    """
    with patch(
        "dfo_pipelines.defs.dfo.ontology.assets.fetch_latest_revision",
        return_value=LATEST_REVISION,
    ) as mock:
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
    tmp_path: Path, *, raise_on_error: bool = True
) -> dg.ExecuteInProcessResult:
    """Observe the source, then materialize the landing asset"""
    return materialize_landing(
        dfo_ontology_landing,
        tmp_path,
        instance=observed_instance(),
        raise_on_error=raise_on_error,
    )


def checks(result: dg.MaterializeResult) -> dict[str, bool]:
    """The asset's checks, keyed on name"""
    return {str(check.check_name): bool(check.passed) for check in result.check_results}


def test_landing_counts_the_nodes_the_mcf_declares(
    tmp_path: Path, compiler: Any
) -> None:
    """Test the node count counts nodes, not the blocks the file separates into"""
    result = materialize_observed(tmp_path)

    assert materialization(result).metadata["node_count"] == dg.IntMetadataValue(
        STUB_MCF.count("Node: ")
    )


def test_landing_raises_when_the_compiler_fails(tmp_path: Path, compiler: Any) -> None:
    """Test a bench the compiler rejects fails the asset, carrying its report

    The compiler writes its account of what is wrong to stderr, which is all a
    failed run has to explain itself with.
    """
    compiler.side_effect = fake_compile(returncode=1)

    with pytest.raises(RuntimeError, match=STUB_REPORT.strip()):
        materialize_observed(tmp_path)


def test_landing_publishes_nothing_when_an_output_is_missing(
    tmp_path: Path, compiler: Any
) -> None:
    """Test a compile missing an output leaves what an earlier one published

    Runs on the same day write into the same directory, so replacing its files
    one by one would leave the ontology of one compile beside the mapping of
    another, with no materialization recording that it happened.
    """
    published = landed_directory(materialize_observed(tmp_path))
    compiler.side_effect = fake_compile(
        mcf="Node: dcid:Replaced\n", omit=(CODE_MAPPING_CSV,)
    )

    with pytest.raises(RuntimeError, match=re.escape(CODE_MAPPING_CSV)):
        materialize_observed(tmp_path)

    assert (published / ONTOLOGY_MCF).read_text() == STUB_MCF


def test_source_versions_the_latest_revision() -> None:
    """Test the source versions the revision id, and reports the API and when the
    revision was saved"""
    observe = dfo_ontology_source.observe_fn
    assert observe is not None

    result = observe()

    assert isinstance(result, dg.ObserveResult)
    assert result.data_version == dg.DataVersion("2982")
    assert result.metadata == {
        "dagster/uri": dg.TextMetadataValue(BENCH.url),
        "version": dg.TextMetadataValue("2982"),
        "revised_at": dg.TextMetadataValue("2026-11-10T13:08:34+00:00"),
    }


def test_landing_records_the_date_of_the_observed_revision_as_the_release_date(
    tmp_path: Path, compiler: Any
) -> None:
    """Test the ontology was released on the day of the observed revision"""
    result = materialize_observed(tmp_path)

    landed = landed_directory(result)
    assert (
        json.loads((landed / METADATA_FILE_NAME).read_text())["source_release_date"]
        == "2026-11-10"
    )


def test_landing_raises_when_the_instance_is_edited_after_the_observation(
    tmp_path: Path, compiler: Any, wikibase: Any
) -> None:
    """Test a compile is refused when the instance changed after it was observed

    The compile may have read the edit, which would then be recorded under the
    observed version and its release date. What an earlier run landed stays.
    """
    published = landed_directory(materialize_observed(tmp_path))
    wikibase.side_effect = [
        LATEST_REVISION,
        dataclasses.replace(
            LATEST_REVISION, revision_id=LATEST_REVISION.revision_id + 1
        ),
    ]
    compiler.side_effect = fake_compile(mcf="Node: dcid:Replaced\n")

    with pytest.raises(RuntimeError, match="edited after it was observed"):
        materialize_observed(tmp_path)

    assert (published / ONTOLOGY_MCF).read_text() == STUB_MCF


def test_fetch_raises_when_the_instance_was_edited_before_the_compile(
    tmp_path: Path, compiler: Any, wikibase: Any
) -> None:
    """Test the fetch compares against the observed revision, so an edit made
    between the observation and the compile is refused too"""
    observation = observe()
    wikibase.return_value = dataclasses.replace(
        LATEST_REVISION, revision_id=LATEST_REVISION.revision_id + 1
    )

    with pytest.raises(RuntimeError, match="edited after it was observed"):
        fetch_ontology(UPath(tmp_path), observation)


@pytest.mark.parametrize(
    "mcf", ["", STUB_MCF.split("\n\n")[0] + "\n"], ids=["empty", "header_only"]
)
def test_landing_check_fails_when_the_mcf_declares_no_nodes(
    tmp_path: Path, compiler: Any, mcf: str
) -> None:
    """Test the files check fails on the MCF when the compiler emits no ontology,
    even with the header comment it writes"""
    compiler.side_effect = fake_compile(mcf=mcf)

    result = materialize_observed(tmp_path, raise_on_error=False)

    [evaluation] = result.get_asset_check_evaluations()
    assert not evaluation.passed
    assert evaluation.metadata["empty_files"] == dg.JsonMetadataValue([ONTOLOGY_MCF])


def test_landing_check_fails_when_the_code_mapping_has_no_rows(
    tmp_path: Path, compiler: Any
) -> None:
    """Test the files check counts the mapping's rows, so a header alone fails it

    A file holding only its header is not empty on disk, but resolves nothing.
    """
    compiler.side_effect = fake_compile(rows=[])

    result = materialize_observed(tmp_path, raise_on_error=False)

    [evaluation] = result.get_asset_check_evaluations()
    assert not evaluation.passed
    assert evaluation.metadata["empty_files"] == dg.JsonMetadataValue(
        [CODE_MAPPING_CSV]
    )


def test_raw_code_mapping_table_loads_every_mapping(tmp_path: Path) -> None:
    """Test the code mapping is loaded whole, for other pipelines to resolve against"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    rows = mapping_rows()

    result = dfo_ontology_raw_code_mapping_table(
        duckdb=duckdb, dfo_ontology_landing=str(FIXTURES)
    )

    assert row_count(duckdb, "dfo_ontology.raw_code_mapping") == len(rows)
    assert column_list(duckdb, "dfo_ontology.raw_code_mapping") == list(rows[0])
    assert isinstance(result, dg.MaterializeResult)
    assert meta(result, "dagster/row_count", dg.IntMetadataValue).value == len(rows)
    assert checks(result) == {"table_not_empty": True}


def test_raw_code_mapping_table_replaces_what_it_finds(tmp_path: Path) -> None:
    """Test a refreshed ontology replaces the table rather than appending to it"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    rows = mapping_rows()
    fewer = rows[: len(rows) // 2]
    write_mapping(tmp_path / "refreshed", fewer)

    dfo_ontology_raw_code_mapping_table(
        duckdb=duckdb, dfo_ontology_landing=str(FIXTURES)
    )
    assert row_count(duckdb, "dfo_ontology.raw_code_mapping") == len(rows)

    dfo_ontology_raw_code_mapping_table(
        duckdb=duckdb, dfo_ontology_landing=str(tmp_path / "refreshed")
    )

    assert row_count(duckdb, "dfo_ontology.raw_code_mapping") == len(fewer)


def test_raw_code_mapping_table_check_fails_when_the_mapping_has_no_rows(
    tmp_path: Path,
) -> None:
    """Test the table check fails when the mapping carries only its header"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    write_mapping(tmp_path / "empty", [])

    result = dfo_ontology_raw_code_mapping_table(
        duckdb=duckdb, dfo_ontology_landing=str(tmp_path / "empty")
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
    return result, Path(str(meta(result, "dagster/uri", dg.TextMetadataValue).value))


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


def test_the_compiled_ontology_reaches_the_table_and_the_bundle(
    tmp_path: Path, compiler: Any
) -> None:
    """Test the directory the compiler writes is what both consumers read"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "landing"))
    export_file_store = UPathFileStoreResource(base_path=str(tmp_path / "processed"))
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    result = dg.materialize(
        [
            dfo_ontology_landing,
            dfo_ontology_raw_code_mapping_table,
            dfo_ontology_dc_export,
        ],
        resources={
            "landing_file_store": file_store,
            "export_file_store": export_file_store,
            "duckdb": duckdb,
        },
        instance=observed_instance(),
    )

    assert result.success
    assert all(check.passed for check in result.get_asset_check_evaluations()), (
        result.get_asset_check_evaluations()
    )
    assert row_count(duckdb, "dfo_ontology.raw_code_mapping") == len(mapping_rows())
