from collections.abc import Sequence

import dagster as dg
from upath import UPath

from dfo_pipelines.data_commons.ingestion import ingest_bundle
from dfo_pipelines.orchestration.resources import KnowledgeGraphResource


def build_dc_ingestion_asset(
    *,
    import_name: str,
    group: str,
    title: str,
    deps: Sequence[dg.AssetsDefinition] = (),
    description_note: str | None = None,
) -> dg.AssetsDefinition:
    """The `<import_name>_dc_ingestion` asset ingesting its export's bundle"""
    description = (
        f"{title} bundle ingested into the Data Commons knowledge graph\n\n"
        "Materialized by hand only: an ingestion rewrites the shared graph and runs\n"
        "for as long as it takes after the workflow is started."
    )
    if description_note is not None:
        description = f"{description} {description_note}"

    # TODO: Make this eager in a single-slot pool once the ingestion step can wait
    # for the workflow to finish: https://github.com/ONEcampaign/dcp-tools/issues/109
    @dg.asset(
        name=f"{import_name}_dc_ingestion",
        description=description,
        ins={"bundle": dg.AssetIn(f"{import_name}_dc_export")},
        group_name=group,
        kinds={"gcs"},
        deps=deps,
    )
    def dc_ingestion(
        context: dg.AssetExecutionContext,
        knowledge_graph: KnowledgeGraphResource,
        bundle: str,
    ) -> dg.MaterializeResult:
        destination = ingest_bundle(
            UPath(bundle),
            import_name=import_name,
            settings=knowledge_graph.settings,
            log=context.log,
        )
        return dg.MaterializeResult(
            metadata={
                "dagster/uri": dg.MetadataValue.text(destination),
                "import_name": dg.MetadataValue.text(import_name),
            },
        )

    return dc_ingestion
