import dagster as dg
from oda_reader import bulk_download_dac2b
from oda_reader.dac2b import DAC2B_BULK_LABEL
from oda_reader.download.download_tools import DAC2B_FLOW_URL
from upath import UPath

from dfo_pipelines.data_commons.export import (
    DOCS_ROOT,
    ConversionRecipe,
    ExportSpec,
    ObservationInput,
)
from dfo_pipelines.defs.dfo.ontology.assets import dfo_ontology_dc_ingestion
from dfo_pipelines.defs.dfo.shared.assets import dfo_shared_dc_ingestion
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
    group_name="oecd_dac2b",
    automation_condition=dg.AutomationCondition.on_cron("@daily"),
)
def oecd_dac2b_source() -> dg.ObserveResult:
    """Observed OECD DAC2b data version.

    The source is observed daily via calls to the OECD API.
    """
    url, version = resolve_bulk_file(flow_url=DAC2B_FLOW_URL, label=DAC2B_BULK_LABEL)

    data_version = dg.DataVersion(";".join([url, version]))

    return dg.ObserveResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(url),
            "version": dg.MetadataValue.text(version),
        },
        data_version=data_version,
    )


def fetch_dac2b(file_path: UPath) -> FetchedRelease:
    """Write the DAC2b bulk file to `file_path`"""
    return fetch_bulk_file(
        file_path,
        flow_url=DAC2B_FLOW_URL,
        label=DAC2B_BULK_LABEL,
        download=bulk_download_dac2b,
        downloaded_name="table2b_data.parquet",
        release_date=lambda url, _: fetch_bulk_file_release_date(url),
    )


oecd_dac2b_landing = build_landing_asset(
    group="oecd_dac2b",
    dataset="dac2b",
    title="OECD DAC2b",
    fetch=fetch_dac2b,
)


oecd_dac2b_raw_table, oecd_dac2b_raw_metadata = build_raw_table_assets(
    group="oecd_dac2b",
    table="raw_dac2b",
    title="OECD DAC2b",
    renames=DAC_COLUMN_RENAMES,
)


OECD_DAC2B_EXPORT_SPEC = ExportSpec(
    import_name="oecd_dac2b",
    provenance_dcid="OECDDAC2B",
    provenance_model="oecd_dac2b.dc_dac2b_provenance",
    conversion_recipe=ConversionRecipe.DAC,
    statvar_model="oecd_dac2b.dc_dac2b_statvars",
    observation_inputs=(
        ObservationInput(
            model="oecd_dac2b.dc_dac2b_observations",
            property_to_column={
                "variable": "variable",
                "date": "date",
                "value": "value",
                "unit": "unit",
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
        "dac2b_aid_type": "dac2bAidType",
    },
    docs_dir=DOCS_ROOT / "oecd" / "dac2b",
    constant_observation_properties={"observationPeriod": "P1Y"},
    provenance_column_to_property={
        "partial_observation_date": "partialObservationDate"
    },
)


oecd_dac2b_dc_export = build_dc_export_asset(
    spec=OECD_DAC2B_EXPORT_SPEC,
    group="oecd_dac2b",
    title="OECD DAC2b",
)


oecd_dac2b_dc_ingestion = build_dc_ingestion_asset(
    import_name=OECD_DAC2B_EXPORT_SPEC.import_name,
    group="oecd_dac2b",
    title="OECD DAC2b",
    deps=[dfo_ontology_dc_ingestion, dfo_shared_dc_ingestion],
)
