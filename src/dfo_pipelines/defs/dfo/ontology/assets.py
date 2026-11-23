import hashlib
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource
from dcp_tools import CustomDataManager
from dfo_ontology.config import Config
from upath import UPath

from dfo_pipelines.data_commons.mcf import count_mcf_nodes
from dfo_pipelines.extraction.metadata import FetchedRelease
from dfo_pipelines.orchestration.automation import EAGER_WHEN_CHECKS_PASS
from dfo_pipelines.orchestration.dc_export import export_directory_result
from dfo_pipelines.orchestration.dc_ingestion import build_dc_ingestion_asset
from dfo_pipelines.orchestration.landing import Observation, build_landing_asset
from dfo_pipelines.orchestration.raw_tables import raw_table_result
from dfo_pipelines.orchestration.resources import UPathFileStoreResource
from dfo_pipelines.orchestration.subprocesses import run_logged

IMPORT_NAME = "dfo_ontology"
ONTOLOGY_MCF = "dfo.mcf"
CODE_MAPPING_CSV = "codes.csv"
COMPILE_REPORT = "report.txt"
COMPILED_FILES = (ONTOLOGY_MCF, CODE_MAPPING_CSV, COMPILE_REPORT)
# The Wikibase instance the ontology is compiled from
BENCH = Config(bench="prod")


def _compile(directory: Path) -> None:
    """Compile the ontology, code mapping and report into `directory`

    Raises:
        RuntimeError: If the compile fails or leaves out a file.
    """
    result = run_logged(
        [
            "dfo-ontology",
            "compile",
            "--bench",
            BENCH.bench,
            "--output",
            str(directory / ONTOLOGY_MCF),
        ]
    )
    if result.returncode != 0:
        raise RuntimeError(f"Compiling the DFO ontology failed:\n{result.stderr}")
    missing = [name for name in COMPILED_FILES if not (directory / name).exists()]
    if missing:
        raise RuntimeError(f"The DFO ontology compiled without {', '.join(missing)}")


def _compiled_version(directory: Path) -> str:
    """The sha256 of the compiled ontology and code mapping, leaving out the
    report"""
    digest = hashlib.sha256()
    for name in (ONTOLOGY_MCF, CODE_MAPPING_CSV):
        digest.update(hashlib.sha256((directory / name).read_bytes()).digest())
    return digest.hexdigest()


@dg.observable_source_asset(
    group_name="dfo_ontology",
    automation_condition=dg.AutomationCondition.on_cron("@daily"),
)
def dfo_ontology_source() -> dg.ObserveResult:
    """Observed hash of the ontology compiled from its Wikibase instance"""
    with tempfile.TemporaryDirectory() as staging_dir:
        staging_path = Path(staging_dir)
        _compile(staging_path)
        version = _compiled_version(staging_path)
    return dg.ObserveResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(BENCH.url),
            "version": dg.MetadataValue.text(version),
        },
        data_version=dg.DataVersion(version),
    )


def fetch_ontology(directory: UPath, observation: Observation) -> FetchedRelease:
    """Write the compiled ontology, code mapping and report to `directory`

    A compile that differs from the observed one is refused, so a change made
    after the observation is never recorded under its version.
    """
    with tempfile.TemporaryDirectory() as staging_dir:
        staging_path = Path(staging_dir)
        _compile(staging_path)
        if _compiled_version(staging_path) != observation.data_version:
            raise RuntimeError(
                f"{BENCH.site} changed after it was observed: observe it again"
            )
        for name in COMPILED_FILES:
            (directory / name).write_bytes((staging_path / name).read_bytes())

    return FetchedRelease(
        release_dates={"source": datetime.now(UTC).date()},
        metadata={},
    )


dfo_ontology_landing = build_landing_asset(
    group="dfo_ontology",
    source=dfo_ontology_source,
    dataset="ontology",
    title="Compiled DFO ontology",
    files=[ONTOLOGY_MCF, CODE_MAPPING_CSV],
    fetch=fetch_ontology,
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
    duckdb: DuckDBResource, dfo_ontology_landing: str
) -> dg.MaterializeResult:
    """Raw DFO ontology code mapping database table"""
    schema = "dfo_ontology"
    table = "raw_code_mapping"
    code_mapping = str(UPath(dfo_ontology_landing) / CODE_MAPPING_CSV)
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
    return raw_table_result(f"{schema}.{table}", rows)


@dg.asset(
    group_name="dfo_ontology",
    automation_condition=EAGER_WHEN_CHECKS_PASS,
)
def dfo_ontology_dc_export(
    context: dg.AssetExecutionContext,
    export_file_store: UPathFileStoreResource,
    dfo_ontology_landing: str,
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
        (UPath(dfo_ontology_landing) / ONTOLOGY_MCF).open("rb"),
        (out_dir / ONTOLOGY_MCF).open("wb"),
    )
    context.log.info(f"Exported the DFO ontology to {out_dir}")
    return export_directory_result(
        out_dir, {"node_count": count_mcf_nodes(out_dir / ONTOLOGY_MCF)}
    )


dfo_ontology_dc_ingestion = build_dc_ingestion_asset(
    import_name=IMPORT_NAME, group="dfo_ontology", title="DFO ontology"
)
