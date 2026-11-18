import dagster as dg
from oda_reader import bulk_download_dac2a
from oda_reader.dac2a import DAC2A_BULK_LABEL
from oda_reader.download.download_tools import DAC2A_FLOW_URL
from upath import UPath

from dfo_pipelines.data_commons.export import (
    DOCS_ROOT,
    ConversionRecipe,
    ExportSpec,
    ObservationInput,
)
from dfo_pipelines.defs.dfo.ontology.assets import dfo_ontology_dc_ingestion
from dfo_pipelines.defs.dfo.shared.assets import dfo_shared_dc_ingestion
from dfo_pipelines.defs.world_bank.ids.assets import world_bank_ids_dc_ingestion
from dfo_pipelines.extraction.metadata import FetchedRelease
from dfo_pipelines.extraction.oecd import (
    DAC_COLUMN_RENAMES,
    fetch_bulk_file,
    fetch_bulk_file_release_date,
    resolve_bulk_file,
)
from dfo_pipelines.orchestration.dc_export import build_dc_export_asset
from dfo_pipelines.orchestration.dc_ingestion import build_dc_ingestion_asset
from dfo_pipelines.orchestration.landing import build_landing_asset
from dfo_pipelines.orchestration.raw_tables import build_raw_table_assets


@dg.observable_source_asset(
    group_name="oecd_dac2a",
    automation_condition=dg.AutomationCondition.on_cron("@daily"),
)
def oecd_dac2a_source() -> dg.ObserveResult:
    """Observed OECD DAC2a data version.

    The source is observed daily via calls to the OECD API.
    """
    url, version = resolve_bulk_file(flow_url=DAC2A_FLOW_URL, label=DAC2A_BULK_LABEL)

    data_version = dg.DataVersion(";".join([url, version]))

    return dg.ObserveResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(url),
            "version": dg.MetadataValue.text(version),
        },
        data_version=data_version,
    )


def fetch_dac2a(file_path: UPath) -> FetchedRelease:
    """Write the DAC2a bulk file to `file_path`"""
    return fetch_bulk_file(
        file_path,
        flow_url=DAC2A_FLOW_URL,
        label=DAC2A_BULK_LABEL,
        download=bulk_download_dac2a,
        downloaded_name="table2a_data.parquet",
        release_date=lambda url, _: fetch_bulk_file_release_date(url),
    )


oecd_dac2a_landing = build_landing_asset(
    group="oecd_dac2a",
    dataset="dac2a",
    title="OECD DAC2a",
    fetch=fetch_dac2a,
)


oecd_dac2a_raw_table, oecd_dac2a_raw_metadata = build_raw_table_assets(
    group="oecd_dac2a",
    table="raw_dac2a",
    title="OECD DAC2a",
    renames=DAC_COLUMN_RENAMES,
)


OECD_DAC2A_EXPORT_SPEC = ExportSpec(
    import_name="oecd_dac2a",
    provenance_dcid="OECDDAC2A",
    provenance_model="oecd_dac2a.dc_dac2a_provenance",
    conversion_recipe=ConversionRecipe.DAC,
    statvar_model="oecd_dac2a.dc_dac2a_statvars",
    observation_inputs=(
        ObservationInput(
            model="oecd_dac2a.dc_dac2a_observations",
            property_to_column={
                "variable": "variable",
                "date": "date",
                "value": "value",
                "unit": "unit",
                "measurementMethod": "measurement_method",
                "custom:financingProvider": "financing_provider",
                "custom:financingRecipient": "financing_recipient",
            },
        ),
    ),
    statvar_column_to_property={
        "name": "name",
        "financial_flow_type": "financialFlowType",
        "netting_basis": "nettingBasis",
        "dac_type_of_finance": "dacTypeOfFinance",
        "dac2a_aid_type": "dac2aAidType",
    },
    docs_dir=DOCS_ROOT / "oecd" / "dac2a",
    constant_observation_properties={"observationPeriod": "P1Y"},
    provenance_column_to_property={
        "partial_observation_date": "partialObservationDate"
    },
)


oecd_dac2a_dc_export = build_dc_export_asset(
    spec=OECD_DAC2A_EXPORT_SPEC,
    group="oecd_dac2a",
    title="OECD DAC2a",
)


oecd_dac2a_dc_ingestion = build_dc_ingestion_asset(
    import_name=OECD_DAC2A_EXPORT_SPEC.import_name,
    group="oecd_dac2a",
    title="OECD DAC2a",
    deps=[
        dfo_ontology_dc_ingestion,
        dfo_shared_dc_ingestion,
        # A share of GNI divides by the GNI StatVar the IDS import defines.
        world_bank_ids_dc_ingestion,
    ],
)
