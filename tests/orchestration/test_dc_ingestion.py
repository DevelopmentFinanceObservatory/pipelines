from unittest.mock import patch

import dagster as dg

from dfo_pipelines.definitions import defs
from dfo_pipelines.orchestration.dc_ingestion import build_dc_ingestion_asset
from dfo_pipelines.orchestration.resources import KnowledgeGraphResource

ONTOLOGY_INGESTION = dg.AssetKey("dfo_ontology_dc_ingestion")


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


def test_build_dc_ingestion_asset_appends_the_note_to_the_description() -> None:
    """Test the note follows the standard description"""
    ingestion = build_dc_ingestion_asset(
        import_name="example",
        group="example",
        title="Example",
        description_note="It follows the ontology's ingestion.",
    )

    assert ingestion.descriptions_by_key[ingestion.key] == (
        "Example bundle ingested into the Data Commons knowledge graph\n\n"
        "Materialized by hand only: an ingestion rewrites the shared graph and runs\n"
        "for as long as it takes after the workflow is started. It follows the "
        "ontology's ingestion."
    )


def test_every_ingestion_is_manual_and_follows_its_export_and_the_ontology() -> None:
    """Test each ingestion runs by hand, since it rewrites the shared graph, reads
    its own pipeline's export, and waits on the ontology's ingestion"""
    graph = defs().resolve_asset_graph()
    ingestions = [
        key
        for key in graph.get_all_asset_keys()
        if key.path[-1].endswith("_dc_ingestion")
    ]

    assert ingestions
    for key in ingestions:
        node = graph.get(key)
        export = dg.AssetKey(key.path[-1].removesuffix("_ingestion") + "_export")
        assert node.automation_condition is None, key
        assert graph.get(export).is_materializable, key
        assert graph.get(export).group_name == node.group_name, key
        assert export in node.parent_keys, key
        if key != ONTOLOGY_INGESTION:
            assert ONTOLOGY_INGESTION in node.parent_keys, key
