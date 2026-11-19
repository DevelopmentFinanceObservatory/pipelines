import re
from datetime import date

import dagster as dg
from oda_reader import bulk_download_crs
from oda_reader.download.download_tools import CRS_FLOW_URL
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
from dfo_pipelines.extraction.oecd import fetch_bulk_file, resolve_bulk_file
from dfo_pipelines.orchestration.dc_export import build_dc_export_asset
from dfo_pipelines.orchestration.dc_ingestion import build_dc_ingestion_asset
from dfo_pipelines.orchestration.landing import build_landing_asset
from dfo_pipelines.orchestration.raw_tables import RawTableSpec, build_raw_table_assets


def parse_crs_release_date(version: str) -> date:
    """Parse the date out of a version token.

    Args:
        version: Version token as oda_reader resolved it, of the form vYYYYMMDD.

    Returns:
        date: The parsed date.

    Raises:
        ValueError: If the token does not carry a date.
    """
    matched = re.fullmatch(r"v(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})", version)
    if matched is None:
        raise ValueError(
            f"CRS version token carries no release date, expected vYYYYMMDD: {version}"
        )
    return date(int(matched["year"]), int(matched["month"]), int(matched["day"]))


CRS_BULK_LABEL = "CRS-Parquet"


@dg.observable_source_asset(
    group_name="oecd_dac_crs",
    automation_condition=dg.AutomationCondition.on_cron("@daily"),
)
def oecd_dac_crs_source() -> dg.ObserveResult:
    """Observed OECD DAC CRS data version.

    The source is observed daily via calls to the OECD API.
    """
    url, version = resolve_bulk_file(flow_url=CRS_FLOW_URL, label=CRS_BULK_LABEL)

    data_version = dg.DataVersion(";".join([url, version]))

    return dg.ObserveResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(url),
            "version": dg.MetadataValue.text(version),
        },
        data_version=data_version,
    )


def fetch_dac_crs(file_path: UPath) -> FetchedRelease:
    """Write the CRS bulk file to `file_path`"""
    return fetch_bulk_file(
        file_path,
        flow_url=CRS_FLOW_URL,
        label=CRS_BULK_LABEL,
        download=bulk_download_crs,
        downloaded_name="CRS.parquet",
        release_date=lambda _, version: parse_crs_release_date(version),
    )


oecd_dac_crs_landing = build_landing_asset(
    group="oecd_dac_crs",
    dataset="dac_crs",
    title="OECD DAC CRS",
    fetch=fetch_dac_crs,
)


oecd_dac_crs_raw_tables = build_raw_table_assets(
    group="oecd_dac_crs",
    title="OECD DAC CRS",
    tables=[
        RawTableSpec(
            asset="oecd_dac_crs_raw_table",
            file_stem="dac_crs",
            table="raw_dac_crs",
        )
    ],
    metadata_table="raw_dac_crs_metadata",
)


CRS_EXPORT_SPEC = ExportSpec(
    import_name="oecd_dac_crs",
    provenance_dcid="OECDCreditorReportingSystem",
    provenance_model="oecd_dac_crs.dc_dac_crs_provenance",
    conversion_recipe=ConversionRecipe.DAC,
    statvar_model="oecd_dac_crs.dc_dac_crs_statvars",
    observation_inputs=(
        ObservationInput(
            model="oecd_dac_crs.dc_dac_crs_observations",
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
        "netting_basis": "nettingBasis",
        "financial_flow_type": "financialFlowType",
        "financial_obligation_type": "financialObligationType",
        "type_of_flow": "crsTypeOfFlow",
        "type_of_finance": "crsTypeOfFinance",
        "cooperation_modality": "crsCooperationModality",
        "purpose_code": "crsPurpose",
    },
    docs_dir=DOCS_ROOT / "oecd" / "dac_crs",
    constant_observation_properties={"observationPeriod": "P1Y"},
    provenance_column_to_property={
        "partial_observation_date": "partialObservationDate"
    },
)


oecd_dac_crs_dc_export = build_dc_export_asset(
    spec=CRS_EXPORT_SPEC,
    group="oecd_dac_crs",
    title="OECD DAC CRS",
)


oecd_dac_crs_dc_ingestion = build_dc_ingestion_asset(
    import_name=CRS_EXPORT_SPEC.import_name,
    group="oecd_dac_crs",
    title="OECD DAC CRS",
    deps=[dfo_ontology_dc_ingestion, dfo_shared_dc_ingestion],
)
