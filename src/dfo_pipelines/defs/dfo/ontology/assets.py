import shutil
import subprocess
import tempfile
from collections.abc import Iterator
from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource
from dcp_tools import CustomDataManager
from upath import UPath

from dfo_pipelines.data_commons.ingestion import ingest_bundle
from dfo_pipelines.defs.automation import EAGER_WHEN_CHECKS_PASS
from dfo_pipelines.defs.resources import (
    KnowledgeGraphResource,
    UPathFileStoreResource,
)

IMPORT_NAME = "dfo_ontology"
ONTOLOGY_MCF = "dfo.mcf"
CODE_MAPPING_CSV = "codes.csv"
COMPILE_REPORT = "report.txt"


def _mcf_node_lines(mcf_path: UPath) -> Iterator[str]:
    """Every `Node: ` line in an MCF file, one per declared node"""
    return (
        line for line in mcf_path.read_text().splitlines() if line.startswith("Node: ")
    )


def count_mcf_nodes(mcf_path: UPath) -> int:
    """Count the nodes an MCF file declares."""
    return sum(1 for _ in _mcf_node_lines(mcf_path))


def mcf_node_dcids(mcf_path: UPath) -> set[str]:
    """Every node dcid an MCF file declares, read from its `Node: ` lines"""
    return {line.removeprefix("Node: ").strip() for line in _mcf_node_lines(mcf_path)}


@dg.asset(
    group_name="dfo_ontology",
    kinds={"file"},
    check_specs=[
        dg.AssetCheckSpec(
            name="mcf_not_empty", asset="dfo_ontology_raw_directory", blocking=True
        ),
        dg.AssetCheckSpec(
            name="csv_not_empty", asset="dfo_ontology_raw_directory", blocking=True
        ),
    ],
)
def dfo_ontology_raw_directory(
    context: dg.AssetExecutionContext, raw_file_store: UPathFileStoreResource
):
    """Compiled DFO ontology MCF, code mapping and validation report"""
    raw_directory = raw_file_store.build_date_partitioned_path("dfo/ontology")
    raw_directory.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as staging_dir:
        staging_path = Path(staging_dir)
        result = subprocess.run(
            [
                "dfo-ontology",
                "compile",
                "--bench",
                "prod",
                "--output",
                str(staging_path / ONTOLOGY_MCF),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Compiling the DFO ontology failed:\n{result.stderr}")
        expected_files = [
            staging_path / name
            for name in (ONTOLOGY_MCF, CODE_MAPPING_CSV, COMPILE_REPORT)
        ]
        missing = [path.name for path in expected_files if not path.exists()]
        if missing:
            raise RuntimeError(
                f"The DFO ontology compiled without {', '.join(missing)}"
            )
        for path in expected_files:
            (raw_directory / path.name).write_bytes(path.read_bytes())

    mcf_path = raw_directory / ONTOLOGY_MCF
    file_size = mcf_path.stat().st_size
    code_rows = len((raw_directory / CODE_MAPPING_CSV).read_text().splitlines()) - 1
    context.log.info(f"Wrote the compiled DFO ontology to {raw_directory}")
    return dg.MaterializeResult(
        value=str(raw_directory),
        metadata={
            "dagster/uri": dg.MetadataValue.text(str(raw_directory)),
            "node_count": dg.MetadataValue.int(count_mcf_nodes(mcf_path)),
        },
        check_results=[
            dg.AssetCheckResult(check_name="mcf_not_empty", passed=(file_size > 0)),
            dg.AssetCheckResult(check_name="csv_not_empty", passed=(code_rows > 0)),
        ],
    )


@dg.asset(
    group_name="dfo_ontology",
    kinds={"duckdb"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    check_specs=[
        dg.AssetCheckSpec(
            name="table_not_empty",
            asset="dfo_ontology_raw_code_mapping_table",
            blocking=True,
        )
    ],
)
def dfo_ontology_raw_code_mapping_table(
    duckdb: DuckDBResource, dfo_ontology_raw_directory: str
) -> dg.MaterializeResult:
    """Raw DFO ontology code mapping database table"""
    schema = "dfo_ontology"
    table = "raw_code_mapping"
    code_mapping = str(UPath(dfo_ontology_raw_directory) / CODE_MAPPING_CSV)
    with duckdb.get_connection() as connection:
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        connection.execute(
            f"CREATE OR REPLACE TABLE {schema}.{table} AS "
            "SELECT * FROM read_csv(?, header = true, all_varchar = true)",
            [code_mapping],
        )
        rows = (connection.sql(f"SELECT COUNT(*) FROM {schema}.{table}").fetchall())[0][
            0
        ]
    return dg.MaterializeResult(
        metadata={
            "dagster/table_name": dg.MetadataValue.text(f"{schema}.{table}"),
            "dagster/row_count": dg.MetadataValue.int(rows),
        },
        check_results=[
            dg.AssetCheckResult(check_name="table_not_empty", passed=(rows > 0))
        ],
    )


@dg.asset(
    group_name="dfo_ontology",
    automation_condition=EAGER_WHEN_CHECKS_PASS,
)
def dfo_ontology_dc_export(
    context: dg.AssetExecutionContext,
    export_file_store: UPathFileStoreResource,
    dfo_ontology_raw_directory: str,
) -> dg.MaterializeResult:
    """Data Commons export for the DFO ontology"""
    out_dir = export_file_store.build_date_partitioned_path(IMPORT_NAME)

    manager = CustomDataManager()
    manager.set_import_name(IMPORT_NAME)

    # FIXME: Source and Provenance should be added to the ontology, not created here
    source_dcid = "dcid:source/ONEData"
    provenance_dcid = "dcid:provenance/DFOOntology"
    manager.add_source(dcid=source_dcid, name="ONE Data", url="https://data.one.org/")
    manager.add_provenance(
        dcid=provenance_dcid,
        name="DFO Ontology",
        description=(
            "The DFO ontology defines the classes, properties and code lists that "
            "describe development finance in the knowledge graph."
        ),
        source=source_dcid,
        url="https://github.com/DevelopmentFinanceObservatory/ontology",
        license="https://developmentfinance.org/docs/about/cite-and-licence",
        license_type="dcid:DFOCiteAndLicense",
    )
    manager.add_mcf_file(file_name="*.mcf", provenance=provenance_dcid)
    manager.export_all(str(out_dir), validate_data=True)
    shutil.copyfileobj(
        (UPath(dfo_ontology_raw_directory) / ONTOLOGY_MCF).open("rb"),
        (out_dir / ONTOLOGY_MCF).open("wb"),
    )
    context.log.info(f"Exported the DFO ontology to {out_dir}")
    return dg.MaterializeResult(
        value=str(out_dir),
        metadata={
            "dagster/uri": dg.MetadataValue.text(str(out_dir)),
            "node_count": dg.MetadataValue.int(count_mcf_nodes(out_dir / ONTOLOGY_MCF)),
        },
    )


# TODO: Make this eager in a single-slot pool once the ingestion step can wait for
# the workflow to finish: https://github.com/ONEcampaign/dcp-tools/issues/109
@dg.asset(group_name="dfo_ontology", kinds={"gcs"})
def dfo_ontology_dc_ingestion(
    context: dg.AssetExecutionContext,
    knowledge_graph: KnowledgeGraphResource,
    dfo_ontology_dc_export: str,
) -> dg.MaterializeResult:
    """DFO ontology bundle ingested into the Data Commons knowledge graph

    Materialized by hand only: an ingestion rewrites the shared graph and runs
    for as long as it takes after the workflow is started.
    """
    destination = ingest_bundle(
        UPath(dfo_ontology_dc_export),
        import_name=IMPORT_NAME,
        settings=knowledge_graph.settings,
        log=context.log,
    )
    return dg.MaterializeResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(destination),
        },
    )
