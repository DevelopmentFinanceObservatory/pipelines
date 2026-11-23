import shutil
import tempfile
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import dagster as dg
import httpx
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
# The Wikibase instance the ontology is compiled from
BENCH = Config(bench="prod")
REQUEST_TIMEOUT_SECONDS = 30


@dg.observable_source_asset(
    group_name="dfo_ontology",
    automation_condition=dg.AutomationCondition.on_cron("@daily"),
)
def dfo_ontology_source() -> dg.ObserveResult:
    """Observed latest revision in the ontology's Wikibase instance"""
    revision = _fetch_latest_revision()
    version = str(revision.revision_id)
    return dg.ObserveResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(BENCH.url),
            "version": dg.MetadataValue.text(version),
            "revised_at": dg.MetadataValue.text(revision.revised_at.isoformat()),
        },
        data_version=dg.DataVersion(version),
    )


@dataclass(frozen=True, kw_only=True)
class LatestRevision:
    """The latest revision of a page in a set of namespaces

    Attributes:
        revision_id: Id of the revision.
        revised_at: Instant the revision was saved.
    """

    revision_id: int
    revised_at: datetime


def fetch_latest_revision(
    *, api_url: str, namespaces: Collection[int]
) -> LatestRevision:
    """The latest revision in `namespaces` at `api_url`

    Args:
        api_url: The instance's `api.php` endpoint.
        namespaces: Namespaces whose revisions are read.

    Raises:
        RuntimeError: If the API returns an error or the namespaces have no
            revisions.
    """
    with httpx.Client(timeout=REQUEST_TIMEOUT_SECONDS) as client:
        response = client.get(
            api_url,
            params={
                "action": "query",
                "format": "json",
                "formatversion": "2",
                "list": "allrevisions",
                "arvnamespace": "|".join(str(namespace) for namespace in namespaces),
                "arvlimit": "1",
                "arvprop": "ids|timestamp",
            },
        )
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        raise RuntimeError(f"{api_url} returned an error: {payload['error']}")
    revisions = payload["query"]["allrevisions"]
    if not revisions:
        raise RuntimeError(f"{api_url} has no revisions in namespaces {namespaces}")
    [revision] = revisions[0]["revisions"]
    return LatestRevision(
        revision_id=revision["revid"],
        revised_at=datetime.fromisoformat(revision["timestamp"]),
    )


def _fetch_latest_revision() -> LatestRevision:
    """The latest revision in the namespaces the compiler reads from its bench"""
    return fetch_latest_revision(api_url=BENCH.url, namespaces=BENCH.namespaces)


def fetch_ontology(directory: UPath, observation: Observation) -> FetchedRelease:
    """Write the compiled ontology, code mapping and report to `directory`

    The instance is read again after the compile, so a compile is never
    recorded under a revision other than the observed one.
    """
    with tempfile.TemporaryDirectory() as staging_dir:
        staging_path = Path(staging_dir)
        result = run_logged(
            [
                "dfo-ontology",
                "compile",
                "--bench",
                BENCH.bench,
                "--output",
                str(staging_path / ONTOLOGY_MCF),
            ]
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
        if str(_fetch_latest_revision().revision_id) != observation.data_version:
            raise RuntimeError(
                f"{BENCH.site} was edited after it was observed: observe it again"
            )
        for path in expected_files:
            (directory / path.name).write_bytes(path.read_bytes())

    return FetchedRelease(
        release_dates={
            "source": datetime.fromisoformat(observation.metadata["revised_at"]).date()
        },
        metadata={
            "node_count": dg.MetadataValue.int(
                count_mcf_nodes(directory / ONTOLOGY_MCF)
            )
        },
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
