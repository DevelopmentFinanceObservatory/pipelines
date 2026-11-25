from datetime import UTC, datetime

import dagster as dg
from dcp_tools import CustomDataManager

from dfo_pipelines.data_commons.mcf import escape_markdown
from dfo_pipelines.defs.dfo.ontology.assets import dfo_ontology_dc_ingestion
from dfo_pipelines.orchestration.automation import EAGER_WHEN_CHECKS_PASS
from dfo_pipelines.orchestration.dc_export import export_directory_result
from dfo_pipelines.orchestration.dc_ingestion import build_dc_ingestion_asset
from dfo_pipelines.resources.file_store import UPathFileStoreResource
from dfo_pipelines.resources.warehouse import WarehouseResource

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
    warehouse: WarehouseResource,
    export_file_store: UPathFileStoreResource,
) -> dg.MaterializeResult:
    """Data Commons export for the nodes more than one pipeline links to

    A node belongs here when more than one pipeline links to it and the ontology
    does not define it.
    """
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

    license_types = warehouse.get_rows(LICENSE_TYPES_SEED)
    properties = warehouse.get_rows(PROPERTIES_SEED)
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
    with export_file_store.write_once_directory(
        IMPORT_NAME,
        created_at=datetime.now(UTC),
        run_id=context.run.run_id,
        log=context.log,
    ) as out_dir:
        manager.export_all(str(out_dir))

        context.log.info(
            f"Exported {len(license_types)} licence types and {len(properties)}"
            f" properties to {out_dir}"
        )
        return export_directory_result(
            out_dir,
            {
                "license_type_count": len(license_types),
                "property_count": len(properties),
            },
        )


dfo_shared_dc_ingestion = build_dc_ingestion_asset(
    import_name=IMPORT_NAME,
    group="dfo_shared",
    title="DFO shared nodes",
    deps=[dfo_ontology_dc_ingestion],
)
