import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import dagster as dg
import httpx
import pandas as pd
import pytest
from dagster_duckdb import DuckDBResource
from imf_reader.config import DataflowDiscoveryError, NoDataError
from imf_reader.weo.api import FlowRef
from upath import UPath

from dfo_pipelines.defs.imf.weo.assets import (
    PUBLICATION_DATE_URL,
    fetch_weo,
    fetch_weo_publication_date,
    imf_weo_dc_export,
    imf_weo_source,
)
from dfo_pipelines.orchestration.resources import UPathFileStoreResource

FIXTURES = Path(__file__).parent / "fixtures"
FIXTURE_ROWS = len(pd.read_parquet(FIXTURES / "data.parquet"))
PUBLICATION_DATE = date(2026, 4, 14)


def return_dataframe(parquet: Path) -> pd.DataFrame:
    """Return dataframe read fixture from parquet file"""
    return pd.read_parquet(parquet)


def test_fetch_weo_pins_the_download_to_the_resolved_release(
    tmp_path: Path,
) -> None:
    """Test the download names its release, so imf_reader cannot roll back to an older one"""
    file_path = UPath(tmp_path / "data.parquet")

    with (
        patch(
            "dfo_pipelines.defs.imf.weo.assets.weo.get_weo_versions",
            return_value=[("April", 2026), ("October", 2025)],
        ),
        patch(
            "dfo_pipelines.defs.imf.weo.assets.weo.fetch_data_with_metadata",
            side_effect=lambda version: return_dataframe(FIXTURES / "data.parquet"),
        ) as fetch,
        patch(
            "dfo_pipelines.defs.imf.weo.assets.fetch_weo_publication_date",
            return_value=PUBLICATION_DATE,
        ) as publication_date,
    ):
        release_dates, metadata = fetch_weo(file_path)

    fetch.assert_called_once_with(version=("April", 2026))
    publication_date.assert_called_once_with(("April", 2026))
    assert len(pd.read_parquet(file_path)) == FIXTURE_ROWS
    assert release_dates == {"source": PUBLICATION_DATE}
    assert metadata == {"version": "April 2026"}


def test_fetch_weo_raises_when_the_release_cannot_be_served(
    tmp_path: Path,
) -> None:
    """Test an unservable release surfaces rather than rolling back to an older one"""
    file_path = UPath(tmp_path / "data.parquet")

    with (
        patch(
            "dfo_pipelines.defs.imf.weo.assets.weo.get_weo_versions",
            return_value=[("April", 2026), ("October", 2025)],
        ),
        patch(
            "dfo_pipelines.defs.imf.weo.assets.weo.fetch_data_with_metadata",
            side_effect=NoDataError("no data for April;2026"),
        ),
        pytest.raises(NoDataError, match="no data for April;2026"),
    ):
        fetch_weo(file_path)

    assert not file_path.exists()


def test_imf_weo_source_observe_fn_returns_metadata_and_data_version() -> None:
    """Test the source's observe fn records the release label and data version"""
    observe = imf_weo_source.observe_fn
    assert observe is not None

    with patch(
        "dfo_pipelines.defs.imf.weo.assets.weo.get_weo_versions",
        return_value=[("October", 2025), ("April", 2025)],
    ):
        result = observe()

    assert isinstance(result, dg.ObserveResult)
    assert result.data_version == dg.DataVersion("October;2025")
    metadata = result.metadata
    assert metadata is not None
    version = metadata["version"]
    assert isinstance(version, dg.TextMetadataValue)
    assert version.value == "October 2025"


def test_imf_weo_source_observe_fn_raises_when_lookup_fails() -> None:
    """Test an unreachable IMF API surfaces rather than being skipped"""
    observe = imf_weo_source.observe_fn
    assert observe is not None

    with (
        patch(
            "dfo_pipelines.defs.imf.weo.assets.weo.get_weo_versions",
            side_effect=DataflowDiscoveryError("no usable WEO dataflow"),
        ),
        pytest.raises(DataflowDiscoveryError, match="no usable WEO dataflow"),
    ):
        observe()


def _publication_date_mocks(response: httpx.Response, requested: list[str]):
    """Serve the dataflow mapping and the attribute request without a network"""
    real_client = httpx.Client

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        return response

    return (
        patch(
            "dfo_pipelines.defs.imf.weo.assets.weo_api._fetch_flow_mapping",
            return_value={("April", 2026): FlowRef(dataflow_id="WEO", version="9.0.0")},
        ),
        patch(
            "dfo_pipelines.defs.imf.weo.assets.httpx.Client",
            side_effect=lambda **kwargs: real_client(
                **kwargs, transport=httpx.MockTransport(handler)
            ),
        ),
    )


def test_fetch_weo_publication_date_reads_the_serving_dataflow() -> None:
    """Test the date comes from the dataflow imf-reader serves the release from"""
    requested: list[str] = []
    csv_body = (
        "STRUCTURE,PUBLICATION_DATE,UPDATE_DATE\n"
        "dataflow,2026-04-14T13:00:00Z,2026-04-15T13:00:00Z\n"
    )
    mapping, client = _publication_date_mocks(
        httpx.Response(200, text=csv_body), requested
    )

    with mapping, client:
        published = fetch_weo_publication_date(("April", 2026))

    assert published == date(2026, 4, 14)
    assert requested == [
        PUBLICATION_DATE_URL.format(dataflow_id="WEO", version="9.0.0")
    ]


def test_fetch_weo_publication_date_raises_on_http_error() -> None:
    """Test a failed request raises rather than recording no date"""
    mapping, client = _publication_date_mocks(httpx.Response(503), [])

    with mapping, client, pytest.raises(httpx.HTTPStatusError):
        fetch_weo_publication_date(("April", 2026))


GDP_STATVAR = (
    "dcid:ValueAdded_EconomicProduction_GrossOfFixedCapitalConsumption_Nominal"
)

DC_MODELS_SQL = f"""
create schema if not exists imf_weo;

create or replace table imf_weo.weo_source as
select
    'source/InternationalMonetaryFund' as dcid,
    'International Monetary Fund (IMF)' as name,
    'https://www.imf.org/' as url;

create or replace table imf_weo.dc_weo_provenance as
select
    'IMF World Economic Outlook (WEO)' as name,
    'source/InternationalMonetaryFund' as source,
    'https://data.imf.org/en/datasets/IMF.RES:WEO' as url,
    '2026-09-22' as imported_source_release_date;

create or replace table imf_weo.dc_weo_statvars as
select
    '{GDP_STATVAR}' as dcid,
    'Gross domestic product (GDP)' as name,
    'dcid:EconomicProduction' as population_type,
    'dcid:valueAdded' as measured_property,
    'dcid:measuredValue' as stat_type,
    'dcid:Nominal' as measurement_qualifier,
    cast(null as varchar) as measurement_denominator,
    'fixedCapitalConsumptionBasis' as constraint_properties,
    cast(null as varchar) as product_tax_basis,
    'dcid:GrossOfFixedCapitalConsumption' as fixed_capital_consumption_basis,
    cast(null as varchar) as cyclical_adjustment,
    cast(null as varchar) as currency_conversion_basis,
    cast(null as varchar) as final_expenditure_type,
    '"NGDP","NGDPD"' as source_indicator_code;

create or replace table imf_weo.dc_weo_observations as
select * from (values
    ('{GDP_STATVAR}', 2024, 'dcid:country/KEN', 1.5, 'dcid:CurrencyKES', null),
    ('{GDP_STATVAR}', 2030, 'dcid:country/KEN', 2.5, 'dcid:CurrencyKES', 'dcid:Estimated')
) as observations (variable, date, observation_about, value, unit, measurement_method);
"""


def _run_dc_export(tmp_path: Path) -> Path:
    """Materialize the export over a small fixture bundle"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    with duckdb.get_connection() as connection:
        connection.execute(DC_MODELS_SQL)
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "processed"))

    with patch("dfo_pipelines.orchestration.resources.date") as mock_date:
        mock_date.today.return_value = date(2026, 10, 1)
        imf_weo_dc_export(
            context=dg.build_asset_context(),
            duckdb=duckdb,
            export_file_store=file_store,
        )

    return tmp_path / "processed" / "imf_weo" / "2026" / "10" / "01"


def test_imf_weo_dc_export_writes_the_import_config(tmp_path: Path) -> None:
    """Test the config registers the files, the country as the entity, and the period"""
    out_dir = _run_dc_export(tmp_path)

    assert json.loads((out_dir / "config.json").read_text()) == {
        "importName": "imf_weo",
        "inputFiles": [
            {
                "pattern": "*.mcf",
                "provenance": "dcid:provenance/IMFWorldEconomicOutlook",
            },
            {
                "pattern": "observations_*.csv",
                "provenance": "dcid:provenance/IMFWorldEconomicOutlook",
                "columnMappings": {
                    "dcid:variableMeasured": "variable",
                    "dcid:observationDate": "date",
                    "dcid:value": "value",
                    "dcid:unit": "unit",
                    "dcid:measurementMethod": "measurement_method",
                    "dcid:observationAbout": "observation_about",
                },
                "observationProperties": {"observationPeriod": "P1Y"},
                "format": "variablePerRow",
            },
        ],
    }


def test_imf_weo_dc_export_defines_the_source_its_provenance_names(
    tmp_path: Path,
) -> None:
    """Test the bundle carries the IMF Source node the Provenance points at"""
    out_dir = _run_dc_export(tmp_path)

    provenance_mcf = (out_dir / "provenance.mcf").read_text()
    assert provenance_mcf.split("\n\n")[0] == (
        "Node: dcid:source/InternationalMonetaryFund\n"
        'name: "International Monetary Fund (IMF)"\n'
        "typeOf: dcid:Source\n"
        'url: "https://www.imf.org/"'
    )
    assert "source: dcid:source/InternationalMonetaryFund\n" in provenance_mcf


def test_imf_weo_dc_export_writes_the_conversion_recipe_onto_the_provenance_node(
    tmp_path: Path,
) -> None:
    """Test the Provenance node names the IMF conversion recipe as its conversionRecipe"""
    out_dir = _run_dc_export(tmp_path)

    provenance = (out_dir / "provenance.mcf").read_text()
    assert "conversionRecipe: dcid:ConversionRecipeIMF\n" in provenance


def test_imf_weo_dc_export_writes_each_property_the_statvar_sets(
    tmp_path: Path,
) -> None:
    """Test a StatVar's constraints and indicator codes become its properties"""
    out_dir = _run_dc_export(tmp_path)

    [node] = (out_dir / "custom_nodes.mcf").read_text().strip().split("\n\n")
    assert node == (
        f"Node: {GDP_STATVAR}\n"
        'name: "Gross domestic product (GDP)"\n'
        "typeOf: dcid:StatisticalVariable\n"
        "statType: dcid:measuredValue\n"
        "populationType: dcid:EconomicProduction\n"
        "measuredProperty: dcid:valueAdded\n"
        "constraintProperties: dcid:fixedCapitalConsumptionBasis\n"
        "measurementQualifier: dcid:Nominal\n"
        "fixedCapitalConsumptionBasis: dcid:GrossOfFixedCapitalConsumption\n"
        'sourceIndicatorCode: "NGDP","NGDPD"'
    )
