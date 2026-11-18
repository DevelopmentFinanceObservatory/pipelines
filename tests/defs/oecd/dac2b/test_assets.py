import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import dagster as dg
from dagster_duckdb import DuckDBResource
from oda_reader import bulk_download_dac2b
from oda_reader.dac2b import DAC2B_BULK_LABEL
from oda_reader.download.download_tools import DAC2B_FLOW_URL
from upath import UPath

from dfo_pipelines.defs.oecd.dac2b.assets import (
    fetch_dac2b,
    oecd_dac2b_dc_export,
    oecd_dac2b_raw_table,
    oecd_dac2b_source,
)
from dfo_pipelines.orchestration.resources import UPathFileStoreResource
from tests.helpers import column_list

FIXTURES = Path(__file__).parent / "fixtures"

# Stand-in for the resolved bulk-download URL: never fetched, the lookup is mocked.
DAC2B_TEST_URL = "https://webfs-dcd.oecd.org/data/dac2b/DAC2b.parquet"

SOURCE_RELEASE_DATE = date(2026, 7, 7)


def test_dac2b_source_observes_the_dac2b_bulk_file() -> None:
    """Test the source looks up the full DAC2b parquet in its dataflow and records its URL and version"""
    observe = oecd_dac2b_source.observe_fn
    assert observe is not None

    with patch(
        "dfo_pipelines.extraction.oecd.get_bulk_file_url_with_version",
        autospec=True,
        return_value=(DAC2B_TEST_URL, "v20260801"),
    ) as lookup:
        result = observe()

    lookup.assert_called_once_with(
        flow_url=DAC2B_FLOW_URL, label=DAC2B_BULK_LABEL, bypass_annotation_cache=True
    )
    assert isinstance(result, dg.ObserveResult)
    assert result.data_version == dg.DataVersion(f"{DAC2B_TEST_URL};v20260801")
    assert result.metadata == {
        "dagster/uri": dg.TextMetadataValue(DAC2B_TEST_URL),
        "version": dg.TextMetadataValue("v20260801"),
    }


def test_fetch_dac2b_dates_the_release_from_the_url() -> None:
    """Test the landing asset fetches the DAC2b bulk file and dates the release from its URL"""
    with (
        patch(
            "dfo_pipelines.defs.oecd.dac2b.assets.fetch_bulk_file", autospec=True
        ) as fetch,
        patch(
            "dfo_pipelines.defs.oecd.dac2b.assets.fetch_bulk_file_release_date",
            return_value=SOURCE_RELEASE_DATE,
        ) as dating,
    ):
        fetch_dac2b(UPath("data.parquet"))
        arguments = dict(fetch.call_args.kwargs)
        assert arguments.pop("release_date")(DAC2B_TEST_URL, "v20260801") == (
            SOURCE_RELEASE_DATE
        )

    dating.assert_called_once_with(DAC2B_TEST_URL)
    assert arguments == {
        "flow_url": DAC2B_FLOW_URL,
        "label": DAC2B_BULK_LABEL,
        "download": bulk_download_dac2b,
        "downloaded_name": "table2b_data.parquet",
    }


def test_dac2b_raw_table_renames_the_columns_that_differ_only_in_case(
    tmp_path: Path,
) -> None:
    """Test the DAC2b raw table gives each case-colliding column pair distinct names"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    oecd_dac2b_raw_table(duckdb=duckdb, landing_file=str(FIXTURES / "data.parquet"))

    assert {
        "recipient_code",
        "recipient_name",
        "donor_code",
        "donor_name",
        "part_code",
        "part_name",
    } <= set(column_list(duckdb, "oecd_dac2b.raw_dac2b"))


GRANT_STATVAR = (
    "dcid:Amount_FinancialFlow_Gross_Disbursement_DACTypeOfFinanceGrants_"
    "DAC2bAidType201_Nominal"
)
INTEREST_STATVAR = (
    "dcid:Amount_FinancialFlow_Gross_InterestPayment_DACTypeOfFinanceNonGrants_"
    "DAC2bAidType297_Nominal"
)

DC_MODELS_SQL = f"""
create schema if not exists oecd_dac2b;

create or replace table oecd_dac2b.dc_dac2b_provenance as
select
    'OECD DAC2b' as name,
    'dc/s/OrganisationForEconomicCo-operationAndDevelopmentOecd' as source,
    'https://webfs-dcd.oecd.org/files/dotStat/DSD_DAC2/Table2b_Data.zip' as url,
    '2026-09-22' as imported_source_release_date,
    null::varchar as partial_observation_date;

create or replace table oecd_dac2b.dc_dac2b_statvars as
select * from (values
    (
        '{GRANT_STATVAR}',
        'Grants, total',
        'dcid:FinancialFlow',
        'dcid:amount',
        'dcid:measuredValue',
        'dcid:Nominal',
        'nettingBasis,financialFlowType,dacTypeOfFinance,dac2bAidType',
        'dcid:Disbursement',
        'dcid:Gross',
        'dcid:DACTypeOfFinanceGrants',
        'dcid:DAC2bAidType201',
        'financingProvider,financingRecipient'
    ),
    (
        '{INTEREST_STATVAR}',
        'Interest received',
        'dcid:FinancialFlow',
        'dcid:amount',
        'dcid:measuredValue',
        'dcid:Nominal',
        'nettingBasis,financialFlowType,dacTypeOfFinance,dac2bAidType',
        'dcid:InterestPayment',
        'dcid:Gross',
        'dcid:DACTypeOfFinanceNonGrants',
        'dcid:DAC2bAidType297',
        'financingProvider,financingRecipient'
    )
) as statvars (
    dcid,
    name,
    population_type,
    measured_property,
    stat_type,
    measurement_qualifier,
    constraint_properties,
    financial_flow_type,
    netting_basis,
    dac_type_of_finance,
    dac2b_aid_type,
    observation_properties
);

create or replace table oecd_dac2b.dc_dac2b_observations as
select * from (values
    ('{GRANT_STATVAR}', 2023, 'dcid:DACCountriesTotal', 'dcid:country/KEN', 'dcid:CurrencyUSD', 1500000.0),
    ('{INTEREST_STATVAR}', 2023, 'dcid:DACCountriesTotal', 'dcid:country/KEN', 'dcid:CurrencyUSD', -23509652.0)
) as observations (variable, date, financing_provider, financing_recipient, unit, value);
"""


def _run_dc_export(tmp_path: Path) -> Path:
    """Materialize the export over a small fixture bundle"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    with duckdb.get_connection() as connection:
        connection.execute(DC_MODELS_SQL)
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "processed"))

    with patch("dfo_pipelines.orchestration.resources.date") as mock_date:
        mock_date.today.return_value = date(2026, 10, 7)
        oecd_dac2b_dc_export(
            context=dg.build_asset_context(),
            duckdb=duckdb,
            export_file_store=file_store,
        )

    return tmp_path / "processed" / "oecd_dac2b" / "2026" / "10" / "07"


def test_oecd_dac2b_dc_export_writes_the_import_config(tmp_path: Path) -> None:
    """Test the config registers flows by provider and recipient, with the period"""
    out_dir = _run_dc_export(tmp_path)

    provenance = "dcid:provenance/OECDDAC2B"
    assert json.loads((out_dir / "config.json").read_text()) == {
        "importName": "oecd_dac2b",
        "inputFiles": [
            {"pattern": "*.mcf", "provenance": provenance},
            {
                "pattern": "observations_*.csv",
                "provenance": provenance,
                "columnMappings": {
                    "dcid:variableMeasured": "variable",
                    "dcid:observationDate": "date",
                    "dcid:value": "value",
                    "dcid:unit": "unit",
                    "custom:financingProvider": "financing_provider",
                    "custom:financingRecipient": "financing_recipient",
                },
                "observationProperties": {"observationPeriod": "P1Y"},
                "format": "variablePerRow",
            },
        ],
    }


def test_oecd_dac2b_dc_export_writes_the_conversion_recipe_onto_the_provenance_node(
    tmp_path: Path,
) -> None:
    """Test the Provenance node names the DAC conversion recipe as its conversionRecipe"""
    out_dir = _run_dc_export(tmp_path)

    provenance = (out_dir / "provenance.mcf").read_text()
    assert "conversionRecipe: dcid:ConversionRecipeDAC\n" in provenance


def test_oecd_dac2b_dc_export_writes_each_property_the_statvar_sets(
    tmp_path: Path,
) -> None:
    """Test a StatVar's constraints, entities and aid type code become its properties"""
    out_dir = _run_dc_export(tmp_path)

    nodes = (out_dir / "custom_nodes.mcf").read_text().strip().split("\n\n")
    assert nodes == [
        f"Node: {GRANT_STATVAR}\n"
        'name: "Grants, total"\n'
        "typeOf: dcid:StatisticalVariable\n"
        "statType: dcid:measuredValue\n"
        "populationType: dcid:FinancialFlow\n"
        "measuredProperty: dcid:amount\n"
        "constraintProperties: dcid:nettingBasis, dcid:financialFlowType, "
        "dcid:dacTypeOfFinance, dcid:dac2bAidType\n"
        "measurementQualifier: dcid:Nominal\n"
        "observationProperties: dcid:financingProvider, dcid:financingRecipient\n"
        "financialFlowType: dcid:Disbursement\n"
        "nettingBasis: dcid:Gross\n"
        "dacTypeOfFinance: dcid:DACTypeOfFinanceGrants\n"
        "dac2bAidType: dcid:DAC2bAidType201",
        f"Node: {INTEREST_STATVAR}\n"
        'name: "Interest received"\n'
        "typeOf: dcid:StatisticalVariable\n"
        "statType: dcid:measuredValue\n"
        "populationType: dcid:FinancialFlow\n"
        "measuredProperty: dcid:amount\n"
        "constraintProperties: dcid:nettingBasis, dcid:financialFlowType, "
        "dcid:dacTypeOfFinance, dcid:dac2bAidType\n"
        "measurementQualifier: dcid:Nominal\n"
        "observationProperties: dcid:financingProvider, dcid:financingRecipient\n"
        "financialFlowType: dcid:InterestPayment\n"
        "nettingBasis: dcid:Gross\n"
        "dacTypeOfFinance: dcid:DACTypeOfFinanceNonGrants\n"
        "dac2bAidType: dcid:DAC2bAidType297",
    ]
