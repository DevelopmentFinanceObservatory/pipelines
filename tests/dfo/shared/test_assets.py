from pathlib import Path
from unittest.mock import patch

import dagster as dg
from dagster_duckdb import DuckDBResource

from dfo_pipelines.defs.dfo.shared.assets import (
    IMPORT_NAME,
    dfo_shared_dc_export,
    dfo_shared_dc_ingestion,
)
from dfo_pipelines.defs.resources import (
    KnowledgeGraphResource,
    UPathFileStoreResource,
)
from tests.helpers import meta

LICENSE_TYPES_SQL = """
create or replace table dfo_shared.license_types as
select * from (values
    (
        'dcid:TestLicense',
        'Test License',
        'Data available under the Test License.',
        'https://example.org/terms'
    )
) as license_types (dcid, name, description, url)
"""


def create_seeds(tmp_path: Path) -> DuckDBResource:
    """A database holding the seeds the export reads, with one licence type"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    with duckdb.get_connection() as connection:
        connection.execute("create schema if not exists dfo_shared")
        connection.execute(LICENSE_TYPES_SQL)
    return duckdb


def test_dc_export_writes_a_node_for_each_license_type(tmp_path: Path) -> None:
    """Test each seed row becomes a licence type node under the import's provenance"""
    result = dfo_shared_dc_export(
        context=dg.build_asset_context(),
        duckdb=create_seeds(tmp_path),
        export_file_store=UPathFileStoreResource(base_path=str(tmp_path / "processed")),
    )

    assert isinstance(result, dg.MaterializeResult)
    out_dir = Path(meta(result, "dagster/uri", dg.TextMetadataValue).value)
    assert (out_dir / "custom_nodes.mcf").read_text().strip() == (
        "Node: dcid:TestLicense\n"
        'name: "Test License"\n'
        "typeOf: dcid:LicenseTypeEnum\n"
        'description: "Data available under the Test License."\n'
        'url: "https://example.org/terms"'
    )
    provenance = (out_dir / "provenance.mcf").read_text()
    assert "Node: dcid:provenance/DFOShared\n" in provenance
    assert meta(result, "license_type_count", dg.IntMetadataValue).value == 1


def test_ingestion_ingests_the_bundle_the_export_wrote(
    tmp_path: Path, knowledge_graph: KnowledgeGraphResource
) -> None:
    """Test the ingestion is handed the export's directory under its import name"""
    destination = f"gs://test-bucket/ingestion/input/{IMPORT_NAME}"

    with patch(
        "dfo_pipelines.defs.dfo.shared.assets.ingest_bundle",
        return_value=destination,
    ) as ingest_bundle:
        result = dg.materialize(
            [dfo_shared_dc_export, dfo_shared_dc_ingestion],
            resources={
                "duckdb": create_seeds(tmp_path),
                "export_file_store": UPathFileStoreResource(
                    base_path=str(tmp_path / "processed")
                ),
                "knowledge_graph": knowledge_graph,
            },
        )

    assert result.success
    [export] = result.asset_materializations_for_node("dfo_shared_dc_export")
    [ingestion] = result.asset_materializations_for_node("dfo_shared_dc_ingestion")
    args, kwargs = ingest_bundle.call_args
    assert str(args[0]) == export.metadata["dagster/uri"].value
    assert kwargs["import_name"] == IMPORT_NAME
    assert kwargs["settings"] == knowledge_graph.settings
    assert ingestion.metadata["dagster/uri"].value == destination
    assert "import_name" not in export.metadata
    assert "import_name" not in ingestion.metadata
