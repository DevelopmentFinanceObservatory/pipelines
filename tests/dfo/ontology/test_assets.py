import csv
import re
import subprocess
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import dagster as dg
import pytest
from dagster_duckdb import DuckDBResource
from upath import UPath

from dfo_pipelines.defs.dfo.ontology.assets import (
    CODE_MAPPING_CSV,
    COMPILE_REPORT,
    IMPORT_NAME,
    ONTOLOGY_MCF,
    dfo_ontology_dc_export,
    dfo_ontology_dc_ingestion,
    dfo_ontology_raw_code_mapping_table,
    dfo_ontology_raw_directory,
    mcf_node_dcids,
)
from dfo_pipelines.defs.resources import (
    KnowledgeGraphResource,
    UPathFileStoreResource,
)
from tests.helpers import column_list, meta, row_count

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
    with patch("dfo_pipelines.defs.subprocesses.subprocess.run") as mock:
        mock.side_effect = fake_compile()
        yield mock


def materialize_raw_directory(tmp_path: Path) -> Any:
    """Run the raw directory asset against a file store under tmp_path"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
    return dfo_ontology_raw_directory(
        context=dg.build_asset_context(), raw_file_store=file_store
    )


def checks(result: dg.MaterializeResult) -> dict[str, bool]:
    """The asset's checks, keyed on name"""
    return {str(check.check_name): bool(check.passed) for check in result.check_results}


def test_mcf_node_dcids_collects_only_the_node_lines(tmp_path: Path) -> None:
    """Test the parser collects each node's dcid, not the property lines under it"""
    mcf_path = tmp_path / ONTOLOGY_MCF
    mcf_path.write_text(STUB_MCF)

    assert mcf_node_dcids(UPath(mcf_path)) == {"dcid:FinancialFlow", "dcid:crsPurpose"}


def test_raw_directory_counts_the_nodes_the_mcf_declares(
    tmp_path: Path, compiler: Any
) -> None:
    """Test the node count counts nodes, not the blocks the file separates into"""
    result = materialize_raw_directory(tmp_path)

    node_count = meta(result, "node_count", dg.IntMetadataValue).value

    assert node_count == STUB_MCF.count("Node: ")


def test_raw_directory_raises_when_the_compiler_fails(
    tmp_path: Path, compiler: Any
) -> None:
    """Test a bench the compiler rejects fails the asset, carrying its report

    The compiler writes its account of what is wrong to stderr, which is all a
    failed run has to explain itself with.
    """
    compiler.side_effect = fake_compile(returncode=1)

    with pytest.raises(RuntimeError, match=STUB_REPORT.strip()):
        materialize_raw_directory(tmp_path)


def test_raw_directory_publishes_nothing_when_an_output_is_missing(
    tmp_path: Path, compiler: Any
) -> None:
    """Test a compile missing an output leaves what an earlier one published

    Runs on the same day write into the same directory, so replacing its files
    one by one would leave the ontology of one compile beside the mapping of
    another, with no materialization recording that it happened.
    """
    published = Path(str(materialize_raw_directory(tmp_path).value))
    compiler.side_effect = fake_compile(
        mcf="Node: dcid:Replaced\n", omit=(CODE_MAPPING_CSV,)
    )

    with pytest.raises(RuntimeError, match=re.escape(CODE_MAPPING_CSV)):
        materialize_raw_directory(tmp_path)

    assert (published / ONTOLOGY_MCF).read_text() == STUB_MCF


def test_raw_directory_check_fails_when_the_mcf_is_empty(
    tmp_path: Path, compiler: Any
) -> None:
    """Test the MCF check fails when the compiler emits no ontology"""
    compiler.side_effect = fake_compile(mcf="")

    result = materialize_raw_directory(tmp_path)

    assert checks(result) == {"mcf_not_empty": False, "csv_not_empty": True}


def test_raw_directory_check_fails_when_the_code_mapping_has_no_rows(
    tmp_path: Path, compiler: Any
) -> None:
    """Test the mapping check counts rows, so a header alone does not pass it

    A file holding only its header is not empty on disk, but resolves nothing.
    """
    compiler.side_effect = fake_compile(rows=[])

    result = materialize_raw_directory(tmp_path)

    assert checks(result) == {"mcf_not_empty": True, "csv_not_empty": False}


def test_raw_code_mapping_table_loads_every_mapping(tmp_path: Path) -> None:
    """Test the code mapping is loaded whole, for other pipelines to resolve against"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    rows = mapping_rows()

    result = dfo_ontology_raw_code_mapping_table(
        duckdb=duckdb, dfo_ontology_raw_directory=str(FIXTURES)
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
        duckdb=duckdb, dfo_ontology_raw_directory=str(FIXTURES)
    )
    assert row_count(duckdb, "dfo_ontology.raw_code_mapping") == len(rows)

    dfo_ontology_raw_code_mapping_table(
        duckdb=duckdb, dfo_ontology_raw_directory=str(tmp_path / "refreshed")
    )

    assert row_count(duckdb, "dfo_ontology.raw_code_mapping") == len(fewer)


def test_raw_code_mapping_table_check_fails_when_the_mapping_has_no_rows(
    tmp_path: Path,
) -> None:
    """Test the table check fails when the mapping carries only its header"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    write_mapping(tmp_path / "empty", [])

    result = dfo_ontology_raw_code_mapping_table(
        duckdb=duckdb, dfo_ontology_raw_directory=str(tmp_path / "empty")
    )

    assert isinstance(result, dg.MaterializeResult)
    assert checks(result) == {"table_not_empty": False}


def build_export(tmp_path: Path) -> tuple[dg.MaterializeResult, Path]:
    """Materialize the export asset against a compiled ontology on disk"""
    raw_directory = tmp_path / "raw"
    raw_directory.mkdir()
    (raw_directory / ONTOLOGY_MCF).write_text(STUB_MCF)
    export_file_store = UPathFileStoreResource(base_path=str(tmp_path / "processed"))

    result = dfo_ontology_dc_export(
        context=dg.build_asset_context(),
        export_file_store=export_file_store,
        dfo_ontology_raw_directory=str(raw_directory),
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
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
    export_file_store = UPathFileStoreResource(base_path=str(tmp_path / "processed"))
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    result = dg.materialize(
        [
            dfo_ontology_raw_directory,
            dfo_ontology_raw_code_mapping_table,
            dfo_ontology_dc_export,
        ],
        resources={
            "raw_file_store": file_store,
            "export_file_store": export_file_store,
            "duckdb": duckdb,
        },
    )

    assert result.success
    assert all(check.passed for check in result.get_asset_check_evaluations()), (
        result.get_asset_check_evaluations()
    )
    assert row_count(duckdb, "dfo_ontology.raw_code_mapping") == len(mapping_rows())


def test_ingestion_ingests_the_bundle_the_export_wrote(
    tmp_path: Path, compiler: Any, knowledge_graph: KnowledgeGraphResource
) -> None:
    """Test the ingestion is handed the export's directory under its import name"""
    destination = f"gs://test-bucket/ingestion/input/{IMPORT_NAME}"

    with patch(
        "dfo_pipelines.defs.dfo.ontology.assets.ingest_bundle",
        return_value=destination,
    ) as ingest_bundle:
        result = dg.materialize(
            [
                dfo_ontology_raw_directory,
                dfo_ontology_dc_export,
                dfo_ontology_dc_ingestion,
            ],
            resources={
                "raw_file_store": UPathFileStoreResource(
                    base_path=str(tmp_path / "raw")
                ),
                "export_file_store": UPathFileStoreResource(
                    base_path=str(tmp_path / "processed")
                ),
                "knowledge_graph": knowledge_graph,
            },
        )

    assert result.success
    [export] = result.asset_materializations_for_node("dfo_ontology_dc_export")
    [ingestion] = result.asset_materializations_for_node("dfo_ontology_dc_ingestion")
    bundle = export.metadata["dagster/uri"].value
    ingest_bundle.assert_called_once()
    args, kwargs = ingest_bundle.call_args
    assert str(args[0]) == bundle
    assert kwargs["import_name"] == IMPORT_NAME
    assert kwargs["settings"] == knowledge_graph.settings
    assert ingestion.metadata["dagster/uri"].value == destination
