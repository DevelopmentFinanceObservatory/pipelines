import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import dagster as dg
import pyarrow.parquet as pq
from dagster_duckdb import DuckDBResource
from oda_reader import bulk_download_dac2a
from oda_reader.dac2a import DAC2A_BULK_LABEL
from oda_reader.download.download_tools import DAC2A_FLOW_URL
from upath import UPath

from dfo_pipelines.defs.oecd.dac2a.assets import (
    fetch_dac2a,
    oecd_dac2a_dc_export,
    oecd_dac2a_raw_tables,
    oecd_dac2a_source,
)
from dfo_pipelines.orchestration.resources import UPathFileStoreResource
from tests.helpers import column_list, load_raw_tables, write_landing_directory

FIXTURES = Path(__file__).parent / "fixtures"

# Stand-in for the resolved bulk-download URL: never fetched, the lookup is mocked.
DAC2A_TEST_URL = "https://webfs-dcd.oecd.org/data/dac2a/DAC2a.parquet"

SOURCE_RELEASE_DATE = date(2026, 7, 7)


def test_dac2a_source_observes_the_dac2a_bulk_file() -> None:
    """Test the source looks up the full DAC2a parquet in its dataflow and records its URL and version"""
    observe = oecd_dac2a_source.observe_fn
    assert observe is not None

    with patch(
        "dfo_pipelines.extraction.oecd.get_bulk_file_url_with_version",
        autospec=True,
        return_value=(DAC2A_TEST_URL, "v20260801"),
    ) as lookup:
        result = observe()

    lookup.assert_called_once_with(
        flow_url=DAC2A_FLOW_URL,
        label="DAC2A full dataset (dotStat format)",
        bypass_annotation_cache=True,
    )
    assert isinstance(result, dg.ObserveResult)
    assert result.data_version == dg.DataVersion(f"{DAC2A_TEST_URL};v20260801")
    assert result.metadata == {
        "dagster/uri": dg.TextMetadataValue(DAC2A_TEST_URL),
        "version": dg.TextMetadataValue("v20260801"),
    }


def test_fetch_dac2a_dates_the_release_from_the_url() -> None:
    """Test the landing asset fetches the DAC2a bulk file and dates the release from its URL"""
    with (
        patch(
            "dfo_pipelines.defs.oecd.dac2a.assets.fetch_bulk_file", autospec=True
        ) as fetch,
        patch(
            "dfo_pipelines.defs.oecd.dac2a.assets.fetch_bulk_file_release_date",
            return_value=SOURCE_RELEASE_DATE,
        ) as dating,
    ):
        fetch_dac2a(UPath("data.parquet"))
        arguments = dict(fetch.call_args.kwargs)
        assert arguments.pop("release_date")(DAC2A_TEST_URL, "v20260801") == (
            SOURCE_RELEASE_DATE
        )

    dating.assert_called_once_with(DAC2A_TEST_URL)
    assert arguments == {
        "flow_url": DAC2A_FLOW_URL,
        "label": DAC2A_BULK_LABEL,
        "download": bulk_download_dac2a,
        "downloaded_name": "table2a_data.parquet",
    }


def test_dac2a_raw_table_renames_the_columns_that_differ_only_in_case(
    tmp_path: Path,
) -> None:
    """Test the DAC2a raw table gives each case-colliding column pair distinct names"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    load_raw_tables(
        oecd_dac2a_raw_tables,
        duckdb=duckdb,
        directory=write_landing_directory(
            tmp_path / "raw",
            tables={"dac2a": pq.read_table(FIXTURES / "data.parquet")},
        ),
    )

    assert {
        "recipient_code",
        "recipient_name",
        "donor_code",
        "donor_name",
        "part_code",
        "part_name",
    } <= set(column_list(duckdb, "oecd_dac2a.raw_dac2a"))


FLOW_STATVAR = (
    "dcid:AttributedAmount_FinancialFlow_Gross_Disbursement_DAC2aAidType106_Nominal"
)
GNI_STATVAR = "dcid:PrimaryIncome_EconomicUnit_GrossOfFixedCapitalConsumption_Nominal"
SHARE_STATVAR = (
    "dcid:Amount_FinancialFlow_Net_Disbursement_DAC2aAidType206_Nominal_"
    "AsAFractionOf_PrimaryIncome_EconomicUnit_GrossOfFixedCapitalConsumption_Nominal"
)

DC_MODELS_SQL = f"""
create schema if not exists oecd_dac2a;

create or replace table oecd_dac2a.dc_dac2a_provenance as
select
    'OECD DAC2a' as name,
    'dc/s/OrganisationForEconomicCo-operationAndDevelopmentOecd' as source,
    'https://webfs-dcd.oecd.org/files/dotStat/DSD_DAC2/Table2a_Data.zip' as url,
    '2026-09-22' as imported_source_release_date,
    null::varchar as partial_observation_date;

create or replace table oecd_dac2a.dc_dac2a_statvars as
select * from (values
    (
        '{FLOW_STATVAR}',
        'Imputed multilateral ODA',
        'dcid:FinancialFlow',
        'dcid:attributedAmount',
        'dcid:measuredValue',
        'dcid:Nominal',
        null,
        'nettingBasis,financialFlowType,dac2aAidType',
        'dcid:Disbursement',
        'dcid:Gross',
        null,
        'dcid:DAC2aAidType106',
        'financingProvider,financingRecipient'
    ),
    (
        '{SHARE_STATVAR}',
        'ODA, total net (% of recipient GNI)',
        'dcid:FinancialFlow',
        'dcid:amount',
        'dcid:measuredValue',
        'dcid:Nominal',
        '{GNI_STATVAR}',
        'nettingBasis,financialFlowType,dac2aAidType',
        'dcid:Disbursement',
        'dcid:Net',
        null,
        'dcid:DAC2aAidType206',
        'financingProvider,financingRecipient'
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
    netting_basis,
    dac_type_of_finance,
    dac2a_aid_type,
    observation_properties
);

create or replace table oecd_dac2a.dc_dac2a_observations as
select * from (values
    ('{FLOW_STATVAR}', 2023, 'dcid:country/FRA', 'dcid:country/KEN', 'dcid:CurrencyUSD', 'dcid:Modelled', 94012210.0),
    ('{SHARE_STATVAR}', 2023, 'dcid:DACCountriesTotal', 'dcid:country/KEN', 'dcid:Percent', null, 1.227)
) as observations (
    variable, date, financing_provider, financing_recipient, unit, measurement_method, value
);
"""


def _run_dc_export(tmp_path: Path) -> Path:
    """Materialize the export over a small fixture bundle"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    with duckdb.get_connection() as connection:
        connection.execute(DC_MODELS_SQL)
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "processed"))

    with patch("dfo_pipelines.orchestration.resources.date") as mock_date:
        mock_date.today.return_value = date(2026, 10, 7)
        oecd_dac2a_dc_export(
            context=dg.build_asset_context(),
            duckdb=duckdb,
            export_file_store=file_store,
        )

    return tmp_path / "processed" / "oecd_dac2a" / "2026" / "10" / "07"


def test_oecd_dac2a_dc_export_writes_the_import_config(tmp_path: Path) -> None:
    """Test the config registers flows by provider and recipient, with the
    measurement method and the period
    """
    out_dir = _run_dc_export(tmp_path)

    provenance = "dcid:provenance/OECDDAC2A"
    assert json.loads((out_dir / "config.json").read_text()) == {
        "importName": "oecd_dac2a",
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
                    "dcid:measurementMethod": "measurement_method",
                    "custom:financingProvider": "financing_provider",
                    "custom:financingRecipient": "financing_recipient",
                },
                "observationProperties": {"observationPeriod": "P1Y"},
                "format": "variablePerRow",
            },
        ],
    }


def test_oecd_dac2a_dc_export_writes_the_conversion_recipe_onto_the_provenance_node(
    tmp_path: Path,
) -> None:
    """Test the Provenance node names the DAC conversion recipe as its conversionRecipe"""
    out_dir = _run_dc_export(tmp_path)

    provenance = (out_dir / "provenance.mcf").read_text()
    assert "conversionRecipe: dcid:ConversionRecipeDAC\n" in provenance


def test_oecd_dac2a_dc_export_writes_each_property_the_statvar_sets(
    tmp_path: Path,
) -> None:
    """Test a StatVar's constraints, denominator, entities and aid type code become
    its properties
    """
    out_dir = _run_dc_export(tmp_path)

    nodes = (out_dir / "custom_nodes.mcf").read_text().strip().split("\n\n")
    assert nodes == [
        f"Node: {SHARE_STATVAR}\n"
        'name: "ODA, total net (% of recipient GNI)"\n'
        "typeOf: dcid:StatisticalVariable\n"
        "statType: dcid:measuredValue\n"
        "populationType: dcid:FinancialFlow\n"
        "measuredProperty: dcid:amount\n"
        "constraintProperties: dcid:nettingBasis, dcid:financialFlowType, "
        "dcid:dac2aAidType\n"
        "measurementQualifier: dcid:Nominal\n"
        f"measurementDenominator: {GNI_STATVAR}\n"
        "observationProperties: dcid:financingProvider, dcid:financingRecipient\n"
        "financialFlowType: dcid:Disbursement\n"
        "nettingBasis: dcid:Net\n"
        "dac2aAidType: dcid:DAC2aAidType206",
        f"Node: {FLOW_STATVAR}\n"
        'name: "Imputed multilateral ODA"\n'
        "typeOf: dcid:StatisticalVariable\n"
        "statType: dcid:measuredValue\n"
        "populationType: dcid:FinancialFlow\n"
        "measuredProperty: dcid:attributedAmount\n"
        "constraintProperties: dcid:nettingBasis, dcid:financialFlowType, "
        "dcid:dac2aAidType\n"
        "measurementQualifier: dcid:Nominal\n"
        "observationProperties: dcid:financingProvider, dcid:financingRecipient\n"
        "financialFlowType: dcid:Disbursement\n"
        "nettingBasis: dcid:Gross\n"
        "dac2aAidType: dcid:DAC2aAidType106",
    ]
