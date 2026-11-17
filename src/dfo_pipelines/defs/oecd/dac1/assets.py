from functools import partial

import dagster as dg
from oda_reader import bulk_download_dac1
from oda_reader.dac1 import DAC1_BULK_LABEL
from oda_reader.download.download_tools import DAC1_FLOW_URL

from dfo_pipelines.data_commons.export import (
    DOCS_ROOT,
    ConversionRecipe,
    ExportSpec,
    ObservationInput,
)
from dfo_pipelines.defs.dfo.ontology.assets import dfo_ontology_dc_ingestion
from dfo_pipelines.defs.dfo.shared.assets import dfo_shared_dc_ingestion
from dfo_pipelines.defs.world_bank.ids.assets import world_bank_ids_dc_ingestion
from dfo_pipelines.extraction.oecd import (
    fetch_bulk_file,
    fetch_bulk_file_release_date,
    resolve_bulk_file,
)
from dfo_pipelines.orchestration.dc_export import build_dc_export_asset
from dfo_pipelines.orchestration.dc_ingestion import build_dc_ingestion_asset
from dfo_pipelines.orchestration.raw_files import build_raw_file_asset
from dfo_pipelines.orchestration.raw_tables import build_raw_table_assets


@dg.observable_source_asset(
    group_name="oecd_dac1",
    automation_condition=dg.AutomationCondition.on_cron("@daily"),
)
def oecd_dac1_source() -> dg.ObserveResult:
    """Observed OECD DAC1 data version.

    The source is observed daily via calls to the OECD API.
    """
    url, version = resolve_bulk_file(flow_url=DAC1_FLOW_URL, label=DAC1_BULK_LABEL)
    return dg.ObserveResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(url),
            "version": dg.MetadataValue.text(version),
        },
        data_version=dg.DataVersion(f"{url};{version}"),
    )


oecd_dac1_raw_file = build_raw_file_asset(
    group="oecd_dac1",
    dataset="dac1",
    title="OECD DAC1",
    fetch=partial(
        fetch_bulk_file,
        flow_url=DAC1_FLOW_URL,
        label=DAC1_BULK_LABEL,
        download=bulk_download_dac1,
        downloaded_name="table1_data.parquet",
        release_date=lambda url, _: fetch_bulk_file_release_date(url),
    ),
)


oecd_dac1_raw_table, oecd_dac1_raw_metadata = build_raw_table_assets(
    group="oecd_dac1",
    table="raw_dac1",
    title="OECD DAC1",
    renames={
        "DONOR": "donor_code",
        "Donor": "donor_name",
        "PART": "part_code",
        "Part": "part_name",
    },
)


OECD_DAC1_EXPORT_SPEC = ExportSpec(
    import_name="oecd_dac1",
    provenance_dcid="OECDDAC1",
    provenance_model="oecd_dac1.dc_dac1_provenance",
    conversion_recipe=ConversionRecipe.DAC,
    statvar_model="oecd_dac1.dc_dac1_statvars",
    observation_inputs=(
        ObservationInput(
            model="oecd_dac1.dc_dac1_flow_observations",
            property_to_column={
                "variable": "variable",
                "date": "date",
                "value": "value",
                "unit": "unit",
                "custom:financingProvider": "financing_provider",
                "custom:financingRecipient": "financing_recipient",
            },
        ),
        ObservationInput(
            model="oecd_dac1.dc_dac1_donor_observations",
            property_to_column={
                "variable": "variable",
                "date": "date",
                "value": "value",
                "unit": "unit",
                "observationAbout": "observation_about",
            },
            file_stem="donor_observations",
        ),
    ),
    statvar_column_to_property={
        "name": "name",
        "financial_flow_type": "financialFlowType",
        "financial_obligation_type": "financialObligationType",
        "netting_basis": "nettingBasis",
        "fixed_capital_consumption_basis": "fixedCapitalConsumptionBasis",
        "dac_type_of_finance": "dacTypeOfFinance",
        "dac1_aid_type": "dac1AidType",
    },
    docs_dir=DOCS_ROOT / "oecd" / "dac1",
    constant_observation_properties={"observationPeriod": "P1Y"},
)


oecd_dac1_dc_export = build_dc_export_asset(
    spec=OECD_DAC1_EXPORT_SPEC,
    group="oecd_dac1",
    title="OECD DAC1",
)


oecd_dac1_dc_ingestion = build_dc_ingestion_asset(
    import_name=OECD_DAC1_EXPORT_SPEC.import_name,
    group="oecd_dac1",
    title="OECD DAC1",
    deps=[
        dfo_ontology_dc_ingestion,
        dfo_shared_dc_ingestion,
        # DAC1 publishes GNI under the StatVar the IDS import defines.
        world_bank_ids_dc_ingestion,
    ],
)
