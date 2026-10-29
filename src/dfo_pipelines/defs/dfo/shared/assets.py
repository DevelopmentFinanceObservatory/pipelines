import dagster as dg
from dagster_duckdb import DuckDBResource
from dcp_tools import CustomDataManager
from upath import UPath

from dfo_pipelines.data_commons.export import get_model_rows
from dfo_pipelines.data_commons.ingestion import ingest_bundle
from dfo_pipelines.data_commons.mcf import escape_markdown
from dfo_pipelines.defs.automation import EAGER_WHEN_CHECKS_PASS
from dfo_pipelines.defs.dfo.ontology.assets import dfo_ontology_dc_ingestion
from dfo_pipelines.defs.resources import (
    KnowledgeGraphResource,
    UPathFileStoreResource,
)

IMPORT_NAME = "dfo_shared"
LICENSE_TYPES_SEED = "dfo_shared.license_types"
PROPERTIES_SEED = "dfo_shared.properties"


@dg.asset(
    group_name="dfo_shared",
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    deps=[
        dg.AssetKey(LICENSE_TYPES_SEED.split(".")),
        dg.AssetKey(PROPERTIES_SEED.split(".")),
    ],
)
def dfo_shared_dc_export(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    export_file_store: UPathFileStoreResource,
) -> dg.MaterializeResult:
    """Data Commons export for the nodes more than one pipeline links to

    A node belongs here when more than one pipeline links to it and the ontology
    does not define it.
    """
    out_dir = export_file_store.build_date_partitioned_path(IMPORT_NAME)
    provenance_dcid = "dcid:provenance/DFOShared"

    manager = CustomDataManager()
    manager.set_import_name(IMPORT_NAME)
    manager.add_provenance(
        dcid=provenance_dcid,
        name="DFO Shared Nodes",
        description=("Nodes that more than one DFO dataset links to."),
        url="https://github.com/DevelopmentFinanceObservatory/pipelines",
        license="https://developmentfinance.org/docs/about/cite-and-licence",
        license_type="dcid:DFOCiteAndLicense",
        # Defined by the ontology import, which must be ingested first.
        source="dcid:source/ONEData",
    )

    with duckdb.get_connection() as connection:
        license_types = get_model_rows(connection, LICENSE_TYPES_SEED)
        properties = get_model_rows(connection, PROPERTIES_SEED)
    for license_type in license_types:
        manager.add_node(
            dcid=license_type["dcid"],
            name=license_type["name"],
            type_of="LicenseTypeEnum",
            description=license_type["description"],
            additional_properties={"url": escape_markdown(license_type["url"])},
        )
    for schema_property in properties:
        manager.add_node(
            dcid=schema_property["dcid"],
            name=schema_property["name"],
            type_of="Property",
            description=schema_property["description"],
            additional_properties={
                "domainIncludes": schema_property["domain"],
                "rangeIncludes": schema_property["range"],
                # Provisional until the ontology defines it.
                "isProvisional": "schema:True",
            },
        )

    manager.add_mcf_file(file_name="*.mcf", provenance=provenance_dcid)
    manager.export_all(str(out_dir))

    context.log.info(
        f"Exported {len(license_types)} licence types and {len(properties)}"
        f" properties to {out_dir}"
    )
    return dg.MaterializeResult(
        value=str(out_dir),
        metadata={
            "dagster/uri": dg.MetadataValue.text(str(out_dir)),
            "license_type_count": dg.MetadataValue.int(len(license_types)),
            "property_count": dg.MetadataValue.int(len(properties)),
        },
    )


# TODO: Make this eager in a single-slot pool once the ingestion step can wait for
# the workflow to finish: https://github.com/ONEcampaign/dcp-tools/issues/109
@dg.asset(group_name="dfo_shared", kinds={"gcs"}, deps=[dfo_ontology_dc_ingestion])
def dfo_shared_dc_ingestion(
    context: dg.AssetExecutionContext,
    knowledge_graph: KnowledgeGraphResource,
    dfo_shared_dc_export: str,
) -> dg.MaterializeResult:
    """DFO shared nodes bundle ingested into the Data Commons knowledge graph

    Materialized by hand only: an ingestion rewrites the shared graph and runs
    for as long as it takes after the workflow is started. It follows the
    ontology's ingestion, which defines the source the provenance names.
    """
    destination = ingest_bundle(
        UPath(dfo_shared_dc_export),
        import_name=IMPORT_NAME,
        settings=knowledge_graph.settings,
        log=context.log,
    )
    return dg.MaterializeResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(destination),
            "import_name": dg.MetadataValue.text(IMPORT_NAME),
        },
    )
