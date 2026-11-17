import csv
import io
from datetime import date, datetime

import dagster as dg
import httpx
from imf_reader import weo
from imf_reader.weo import api as weo_api
from upath import UPath

from dfo_pipelines.data_commons.export import (
    DOCS_ROOT,
    ConversionRecipe,
    ExportSpec,
    ObservationInput,
)
from dfo_pipelines.defs.dfo.ontology.assets import dfo_ontology_dc_ingestion
from dfo_pipelines.defs.dfo.shared.assets import dfo_shared_dc_ingestion
from dfo_pipelines.orchestration.dc_export import build_dc_export_asset
from dfo_pipelines.orchestration.dc_ingestion import build_dc_ingestion_asset
from dfo_pipelines.orchestration.raw_files import RawFileFetch, build_raw_file_asset
from dfo_pipelines.orchestration.raw_tables import build_raw_table_assets

# One observation of one series, with the dataflow's attributes, which include
# the date the IMF published the release.
PUBLICATION_DATE_URL = (
    "https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.RES/"
    "{dataflow_id}/{version}/USA.NGDP_RPCH.A?attributes=dataset&lastNObservations=1"
)

PUBLICATION_DATE_TIMEOUT_SECONDS = 30


@dg.observable_source_asset(
    group_name="imf_weo",
    automation_condition=dg.AutomationCondition.on_cron("@daily"),
)
def imf_weo_source() -> dg.ObserveResult:
    """Observed IMF WEO release.

    The source is observed daily via the IMF SDMX API so downstream assets
    only run when a new WEO release is published.
    """
    versions = weo.get_weo_versions()
    latest = versions[0]
    month, year = latest
    return dg.ObserveResult(
        metadata={"version": dg.MetadataValue.text(f"{month} {year}")},
        data_version=dg.DataVersion(f"{month};{year}"),
    )


def fetch_weo_raw_file(file_path: UPath) -> RawFileFetch:
    """Write the latest WEO release to `file_path`"""
    # 'version' is resolved and passed explicitly because with version=None,
    # imf_reader rolls back up to three releases when the newest one is listed
    # in the dataflow catalogue but not yet servable (reader.py:160). This would
    # return the older frame with no error — the gate would then hold a stale
    # release under the new release's data version until the next publication.
    version = weo.get_weo_versions()[0]
    dataframe = weo.fetch_data_with_metadata(version=version)
    dataframe.to_parquet(file_path, compression="gzip")
    return RawFileFetch(
        release_dates={"source": fetch_weo_publication_date(version)},
        metadata={"version": dg.MetadataValue.text(f"{version[0]} {version[1]}")},
    )


imf_weo_raw_file = build_raw_file_asset(
    group="imf_weo",
    dataset="weo",
    title="IMF WEO",
    fetch=fetch_weo_raw_file,
    retry_policy=dg.RetryPolicy(max_retries=1),
)


imf_weo_raw_table, imf_weo_raw_metadata = build_raw_table_assets(
    group="imf_weo",
    table="raw_weo",
    title="IMF WEO",
)


# TODO: Use imf-reader's own lookup once it has a public one; this reads its
# private dataflow mapping: https://github.com/ONEcampaign/imf-reader/issues/44
def fetch_weo_publication_date(version: tuple[str, int]) -> date:
    """The date the IMF published a WEO release, from the dataflow serving it"""
    flow = weo_api._fetch_flow_mapping()[version]
    url = PUBLICATION_DATE_URL.format(
        dataflow_id=flow.dataflow_id, version=flow.version
    )
    with httpx.Client(timeout=PUBLICATION_DATE_TIMEOUT_SECONDS) as client:
        response = client.get(url, headers={"Accept": "text/csv"})
        response.raise_for_status()
    [observation] = csv.DictReader(io.StringIO(response.text))
    return datetime.fromisoformat(observation["PUBLICATION_DATE"]).date()


IMF_WEO_EXPORT_SPEC = ExportSpec(
    import_name="imf_weo",
    provenance_dcid="IMFWorldEconomicOutlook",
    provenance_model="imf_weo.dc_weo_provenance",
    conversion_recipe=ConversionRecipe.IMF,
    statvar_model="imf_weo.dc_weo_statvars",
    observation_inputs=(
        ObservationInput(
            model="imf_weo.dc_weo_observations",
            property_to_column={
                "variable": "variable",
                "date": "date",
                "value": "value",
                "unit": "unit",
                "measurementMethod": "measurement_method",
                "observationAbout": "observation_about",
            },
        ),
    ),
    source_model="imf_weo.weo_source",
    statvar_column_to_property={
        "name": "name",
        "source_indicator_code": "sourceIndicatorCode",
        "product_tax_basis": "productTaxBasis",
        "fixed_capital_consumption_basis": "fixedCapitalConsumptionBasis",
        "cyclical_adjustment": "cyclicalAdjustment",
        "currency_conversion_basis": "currencyConversionBasis",
        "final_expenditure_type": "finalExpenditureType",
    },
    docs_dir=DOCS_ROOT / "imf" / "weo",
    constant_observation_properties={"observationPeriod": "P1Y"},
)


imf_weo_dc_export = build_dc_export_asset(
    spec=IMF_WEO_EXPORT_SPEC,
    group="imf_weo",
    title="IMF WEO",
)


imf_weo_dc_ingestion = build_dc_ingestion_asset(
    import_name=IMF_WEO_EXPORT_SPEC.import_name,
    group="imf_weo",
    title="IMF WEO",
    deps=[dfo_ontology_dc_ingestion, dfo_shared_dc_ingestion],
)
