from unittest.mock import patch

import dagster as dg

from dfo_pipelines.definitions import defs
from dfo_pipelines.orchestration.dc_ingestion import build_dc_ingestion_asset
from dfo_pipelines.orchestration.resources import KnowledgeGraphResource

ONTOLOGY_INGESTION = dg.AssetKey("dfo_ontology_dc_ingestion")
SHARED_INGESTION = dg.AssetKey("dfo_shared_dc_ingestion")
# The ingestions that define what the shared nodes link to, so precede them.
INGESTIONS_BEFORE_SHARED = {
    ONTOLOGY_INGESTION,
    SHARED_INGESTION,
    dg.AssetKey("dfo_skills_dc_ingestion"),
}


def test_build_dc_ingestion_asset_ingests_the_bundle_the_export_wrote(
    knowledge_graph: KnowledgeGraphResource,
) -> None:
    """Test the ingestion hands the export's directory to `ingest_bundle` under its
    import name, and reports where the bundle was staged"""
    bundle = "data/processed/example/2026/01/01"
    destination = "gs://test-bucket/ingestion/input/example"

    @dg.asset(group_name="example")
    def example_dc_export() -> str:
        return bundle

    example_dc_ingestion = build_dc_ingestion_asset(
        import_name="example", group="example", title="Example"
    )

    with patch(
        "dfo_pipelines.orchestration.dc_ingestion.ingest_bundle",
        return_value=destination,
    ) as ingest_bundle:
        result = dg.materialize(
            [example_dc_export, example_dc_ingestion],
            resources={"knowledge_graph": knowledge_graph},
        )

    assert result.success
    [ingestion] = result.asset_materializations_for_node("example_dc_ingestion")
    ingest_bundle.assert_called_once()
    args, kwargs = ingest_bundle.call_args
    assert str(args[0]) == bundle
    assert kwargs["import_name"] == "example"
    assert kwargs["settings"] == knowledge_graph.settings
    assert ingestion.metadata == {
        "dagster/uri": dg.TextMetadataValue(destination),
        "import_name": dg.TextMetadataValue("example"),
    }
    assert example_dc_ingestion.specs_by_key[example_dc_ingestion.key].kinds == {"gcs"}


def test_every_export_has_an_ingestion_run_by_hand_after_the_shared_nodes() -> None:
    """Test each export has an ingestion, which runs by hand, since it rewrites the
    shared graph, reads its own pipeline's export, and waits on the ontology's and
    the shared nodes' ingestions"""
    graph = defs().resolve_asset_graph()
    names = {key.path[-1] for key in graph.get_all_asset_keys()}
    imports = {
        name.removesuffix("_dc_export") for name in names if name.endswith("_dc_export")
    }

    assert {name for name in names if name.endswith("_dc_ingestion")} == {
        f"{import_name}_dc_ingestion" for import_name in imports
    }
    for import_name in imports:
        key = dg.AssetKey(f"{import_name}_dc_ingestion")
        node = graph.get(key)
        export = graph.get(dg.AssetKey(f"{import_name}_dc_export"))
        assert node.automation_condition is None, key
        assert export.is_materializable, key
        assert export.group_name == node.group_name, key
        assert export.key in node.parent_keys, key
        if key != ONTOLOGY_INGESTION:
            assert ONTOLOGY_INGESTION in node.parent_keys, key
        if key not in INGESTIONS_BEFORE_SHARED:
            assert SHARED_INGESTION in node.parent_keys, key
