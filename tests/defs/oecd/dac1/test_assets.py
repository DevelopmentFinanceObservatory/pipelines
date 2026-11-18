import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import dagster as dg
from dagster_duckdb import DuckDBResource
from oda_reader import bulk_download_dac1
from oda_reader.dac1 import DAC1_BULK_LABEL
from oda_reader.download.download_tools import DAC1_FLOW_URL
from upath import UPath

from dfo_pipelines.defs.oecd.dac1.assets import (
    fetch_dac1,
    oecd_dac1_dc_export,
    oecd_dac1_raw_table,
    oecd_dac1_source,
)
from dfo_pipelines.orchestration.resources import UPathFileStoreResource
from tests.helpers import column_list

FIXTURES = Path(__file__).parent / "fixtures"

# Stand-in for the resolved bulk-download URL: never fetched, the lookup is mocked.
DAC1_TEST_URL = "https://webfs-dcd.oecd.org/data/dac1/DAC1.parquet"

SOURCE_RELEASE_DATE = date(2026, 7, 7)


def test_dac1_source_observes_the_dac1_bulk_file() -> None:
    """Test the source looks up the full DAC1 parquet in its dataflow and records its URL and version"""
    observe = oecd_dac1_source.observe_fn
    assert observe is not None

    with patch(
        "dfo_pipelines.extraction.oecd.get_bulk_file_url_with_version",
        autospec=True,
        return_value=(DAC1_TEST_URL, "v20260801"),
    ) as lookup:
        result = observe()

    lookup.assert_called_once_with(
        flow_url=DAC1_FLOW_URL,
        label="DAC1 full dataset (dotStat format)",
        bypass_annotation_cache=True,
    )
    assert isinstance(result, dg.ObserveResult)
    assert result.data_version == dg.DataVersion(f"{DAC1_TEST_URL};v20260801")
    assert result.metadata == {
        "dagster/uri": dg.TextMetadataValue(DAC1_TEST_URL),
        "version": dg.TextMetadataValue("v20260801"),
    }


def test_fetch_dac1_dates_the_release_from_the_url() -> None:
    """Test the landing asset fetches the DAC1 bulk file and dates the release from its URL"""
    with (
        patch(
            "dfo_pipelines.defs.oecd.dac1.assets.fetch_bulk_file", autospec=True
        ) as fetch,
        patch(
            "dfo_pipelines.defs.oecd.dac1.assets.fetch_bulk_file_release_date",
            return_value=SOURCE_RELEASE_DATE,
        ) as dating,
    ):
        fetch_dac1(UPath("data.parquet"))
        arguments = dict(fetch.call_args.kwargs)
        assert arguments.pop("release_date")(DAC1_TEST_URL, "v20260801") == (
            SOURCE_RELEASE_DATE
        )

    dating.assert_called_once_with(DAC1_TEST_URL)
    assert arguments == {
        "flow_url": DAC1_FLOW_URL,
        "label": DAC1_BULK_LABEL,
        "download": bulk_download_dac1,
        "downloaded_name": "table1_data.parquet",
    }


def test_dac1_raw_table_renames_the_columns_that_differ_only_in_case(
    tmp_path: Path,
) -> None:
    """Test the DAC1 raw table gives each case-colliding column pair distinct names"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    oecd_dac1_raw_table(duckdb=duckdb, landing_file=str(FIXTURES / "data.parquet"))

    assert {
        "donor_code",
        "donor_name",
        "part_code",
        "part_name",
    } <= set(column_list(duckdb, "oecd_dac1.raw_dac1"))


FLOW_STATVAR = "dcid:Amount_FinancialFlow_Net_Disbursement_DAC1AidType8864_Nominal"
GNI_STATVAR = "dcid:PrimaryIncome_EconomicUnit_GrossOfFixedCapitalConsumption_Nominal"
SHARE_STATVAR = (
    "dcid:GrantEquivalent_FinancialFlow_Disbursement_DAC1AidType1010_Nominal_"
    "AsAFractionOf_PrimaryIncome_EconomicUnit_GrossOfFixedCapitalConsumption_Nominal"
)

DC_MODELS_SQL = f"""
create schema if not exists oecd_dac1;

create or replace table oecd_dac1.dc_dac1_provenance as
select
    'OECD DAC1' as name,
    'dc/s/OrganisationForEconomicCo-operationAndDevelopmentOecd' as source,
    'https://webfs-dcd.oecd.org/files/dotStat/DSD_DAC1/Table1_Data.zip' as url,
    '2026-09-22' as imported_source_release_date;

create or replace table oecd_dac1.dc_dac1_statvars as
select * from (values
    (
        '{FLOW_STATVAR}',
        '1.e. of which: humanitarian aid, net disbursements',
        'dcid:FinancialFlow',
        'dcid:amount',
        'dcid:measuredValue',
        'dcid:Nominal',
        null,
        'nettingBasis,financialFlowType,dac1AidType',
        'dcid:Disbursement',
        null,
        'dcid:Net',
        null,
        null,
        'dcid:DAC1AidType8864',
        'financingProvider,financingRecipient'
    ),
    (
        '{SHARE_STATVAR}',
        'Official Development Assistance (ODA), grant equivalents, as a '
        || 'percentage of GNI',
        'dcid:FinancialFlow',
        'dcid:grantEquivalent',
        'dcid:measuredValue',
        'dcid:Nominal',
        '{GNI_STATVAR}',
        'financialFlowType,dac1AidType',
        'dcid:Disbursement',
        null,
        null,
        null,
        null,
        'dcid:DAC1AidType1010',
        'financingProvider'
    )
) as statvars (
    dcid,
    name,
    population_type,
    measured_property,
    stat_type,
    measurement_qualifier,
    measurement_denominator,
    constraint_properties,
    financial_flow_type,
    financial_obligation_type,
    netting_basis,
    fixed_capital_consumption_basis,
    dac_type_of_finance,
    dac1_aid_type,
    observation_properties
);

create or replace table oecd_dac1.dc_dac1_flow_observations as
select * from (values
    ('{FLOW_STATVAR}', 2024, 'dcid:country/USA', 'dcid:country/PSE', 208530512.35, 'dcid:CurrencyUSD'),
    ('{SHARE_STATVAR}', 2024, 'dcid:country/USA', null, 0.225381, 'dcid:Percent')
) as observations (variable, date, financing_provider, financing_recipient, value, unit);

create or replace table oecd_dac1.dc_dac1_donor_observations as
select * from (values
    ('{GNI_STATVAR}', 2024, 'dcid:country/USA', 29050950000000.0, 'dcid:CurrencyUSD'),
    ('dcid:Count_Person', 2024, 'dcid:country/USA', 340110000.0, null)
) as observations (variable, date, observation_about, value, unit);
"""


def _run_dc_export(tmp_path: Path) -> Path:
    """Materialize the export over a small fixture bundle"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    with duckdb.get_connection() as connection:
        connection.execute(DC_MODELS_SQL)
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "processed"))

    with patch("dfo_pipelines.orchestration.resources.date") as mock_date:
        mock_date.today.return_value = date(2026, 10, 2)
        oecd_dac1_dc_export(
            context=dg.build_asset_context(),
            duckdb=duckdb,
            export_file_store=file_store,
        )

    return tmp_path / "processed" / "oecd_dac1" / "2026" / "10" / "02"


def test_oecd_dac1_dc_export_writes_the_import_config(tmp_path: Path) -> None:
    """Test the config registers flows by provider and recipient, and GNI and
    population about the provider, each with the period
    """
    out_dir = _run_dc_export(tmp_path)

    provenance = "dcid:provenance/OECDDAC1"
    columns = {
        "dcid:variableMeasured": "variable",
        "dcid:observationDate": "date",
        "dcid:value": "value",
        "dcid:unit": "unit",
    }
    period = {"observationPeriod": "P1Y"}
    assert json.loads((out_dir / "config.json").read_text()) == {
        "importName": "oecd_dac1",
        "inputFiles": [
            {"pattern": "*.mcf", "provenance": provenance},
            {
                "pattern": "observations_*.csv",
                "provenance": provenance,
                "columnMappings": columns
                | {
                    "custom:financingProvider": "financing_provider",
                    "custom:financingRecipient": "financing_recipient",
                },
                "observationProperties": period,
                "format": "variablePerRow",
            },
            {
                "pattern": "donor_observations_*.csv",
                "provenance": provenance,
                "columnMappings": columns
                | {"dcid:observationAbout": "observation_about"},
                "observationProperties": period,
                "format": "variablePerRow",
            },
        ],
    }


def test_oecd_dac1_dc_export_writes_the_conversion_recipe_onto_the_provenance_node(
    tmp_path: Path,
) -> None:
    """Test the Provenance node names the DAC conversion recipe as its conversionRecipe"""
    out_dir = _run_dc_export(tmp_path)

    provenance = (out_dir / "provenance.mcf").read_text()
    assert "conversionRecipe: dcid:ConversionRecipeDAC\n" in provenance


def test_oecd_dac1_dc_export_writes_each_property_the_statvar_sets(
    tmp_path: Path,
) -> None:
    """Test a StatVar's constraints, denominator, entities and aid type code become
    its properties
    """
    out_dir = _run_dc_export(tmp_path)

    nodes = (out_dir / "custom_nodes.mcf").read_text().strip().split("\n\n")
    assert nodes == [
        f"Node: {FLOW_STATVAR}\n"
        'name: "1.e. of which: humanitarian aid, net disbursements"\n'
        "typeOf: dcid:StatisticalVariable\n"
        "statType: dcid:measuredValue\n"
        "populationType: dcid:FinancialFlow\n"
        "measuredProperty: dcid:amount\n"
        "constraintProperties: dcid:nettingBasis, dcid:financialFlowType, "
        "dcid:dac1AidType\n"
        "measurementQualifier: dcid:Nominal\n"
        "observationProperties: dcid:financingProvider, dcid:financingRecipient\n"
        "financialFlowType: dcid:Disbursement\n"
        "nettingBasis: dcid:Net\n"
        "dac1AidType: dcid:DAC1AidType8864",
        f"Node: {SHARE_STATVAR}\n"
        'name: "Official Development Assistance (ODA), grant equivalents, as a '
        'percentage of GNI"\n'
        "typeOf: dcid:StatisticalVariable\n"
        "statType: dcid:measuredValue\n"
        "populationType: dcid:FinancialFlow\n"
        "measuredProperty: dcid:grantEquivalent\n"
        "constraintProperties: dcid:financialFlowType, dcid:dac1AidType\n"
        "measurementQualifier: dcid:Nominal\n"
        f"measurementDenominator: {GNI_STATVAR}\n"
        "observationProperties: dcid:financingProvider\n"
        "financialFlowType: dcid:Disbursement\n"
        "dac1AidType: dcid:DAC1AidType1010",
    ]
