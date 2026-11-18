import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import dagster as dg
import pytest
from dagster_duckdb import DuckDBResource
from oda_reader import bulk_download_crs
from oda_reader.download.download_tools import CRS_FLOW_URL
from upath import UPath

from dfo_pipelines.defs.oecd.dac_crs.assets import (
    CRS_BULK_LABEL,
    fetch_dac_crs_landing_file,
    oecd_dac_crs_dc_export,
    oecd_dac_crs_source,
    parse_crs_release_date,
)
from dfo_pipelines.orchestration.resources import UPathFileStoreResource

# Stand-in for the resolved bulk-download URL: never fetched, the lookup is mocked.
CRS_TEST_URL = "https://webfs-dcd.oecd.org/data/dac_crs/CRS.parquet"

SOURCE_VERSION = "v20260801"
SOURCE_RELEASE_DATE = date(2026, 8, 1)


def test_parse_crs_release_date_reads_the_date_from_the_version_token() -> None:
    """Test the publication date is read out of a vYYYYMMDD token"""
    assert parse_crs_release_date(SOURCE_VERSION) == SOURCE_RELEASE_DATE


@pytest.mark.parametrize(
    "version",
    ["20260801", "v2026080", "vYYYYMMDD", "v20260801\n"],
    ids=["no prefix", "wrong length", "not digits", "trailing newline"],
)
def test_parse_crs_release_date_raises_when_the_token_carries_no_date(
    version: str,
) -> None:
    """Test a changed token format fails rather than recording a wrong release

    The token is the only statement of when OECD published the file, so a format
    that no longer carries a date has to stop the extraction.
    """
    with pytest.raises(ValueError, match="vYYYYMMDD"):
        parse_crs_release_date(version)


def test_dac_crs_source_observes_the_crs_bulk_file() -> None:
    """Test the source looks up the full CRS parquet in its dataflow and records its URL and version"""
    observe = oecd_dac_crs_source.observe_fn
    assert observe is not None

    with patch(
        "dfo_pipelines.extraction.oecd.get_bulk_file_url_with_version",
        autospec=True,
        return_value=(CRS_TEST_URL, "v20260801"),
    ) as lookup:
        result = observe()

    lookup.assert_called_once_with(
        flow_url=CRS_FLOW_URL, label="CRS-Parquet", bypass_annotation_cache=True
    )
    assert isinstance(result, dg.ObserveResult)
    assert result.data_version == dg.DataVersion(f"{CRS_TEST_URL};v20260801")
    assert result.metadata == {
        "dagster/uri": dg.TextMetadataValue(CRS_TEST_URL),
        "version": dg.TextMetadataValue("v20260801"),
    }


def test_dac_crs_landing_file_dates_the_release_from_the_version() -> None:
    """Test the landing asset fetches the CRS bulk file and dates the release from its
    version token"""
    with patch(
        "dfo_pipelines.defs.oecd.dac_crs.assets.fetch_bulk_file", autospec=True
    ) as fetch:
        fetch_dac_crs_landing_file(UPath("data.parquet"))

    arguments = dict(fetch.call_args.kwargs)
    assert arguments.pop("release_date")(CRS_TEST_URL, SOURCE_VERSION) == (
        SOURCE_RELEASE_DATE
    )
    assert arguments == {
        "flow_url": CRS_FLOW_URL,
        "label": CRS_BULK_LABEL,
        "download": bulk_download_crs,
        "downloaded_name": "CRS.parquet",
    }


COMMITMENT_STATVAR = "dcid:Amount_FinancialObligation_Commitment_CRSTypeOfFlow10_CRSPurposeCode11110_Nominal"
DISBURSEMENT_STATVAR = "dcid:Amount_FinancialFlow_Disbursement_Gross_CRSTypeOfFlow10_CRSPurposeCode11110_Nominal"

PROVENANCE_SQL = """
create or replace table oecd_dac_crs.dc_dac_crs_provenance as
select
    'Test Provenance' as name,
    'dc/s/TestSource' as source,
    'https://example.org/crs' as url,
    'https://example.org/crs/data.parquet' as source_data_url,
    'https://example.org/terms' as license,
    'dcid:TestLicenseType' as license_type,
    'P1Y' as source_release_frequency,
    '2026-09-22' as imported_source_release_date,
    '2026-12' as next_source_release_date,
    '1973' as earliest_observation_date,
    '1974' as latest_observation_date,
    '1974' as partial_observation_date,
    'Test Source (2026), Test Provenance (accessed on 2026-08-18)'
        as license_attribution
"""

STATVARS_SQL = f"""
create or replace table oecd_dac_crs.dc_dac_crs_statvars as
select * from (values
    (
        '{COMMITMENT_STATVAR}',
        'dcid:FinancialObligation',
        'dcid:amount',
        'dcid:measuredValue',
        'financialObligationType,crsTypeOfFlow,crsTypeOfFinance,crsCooperationModality,crsPurpose',
        'dcid:CRSTypeOfFlow10',
        'dcid:CRSTypeOfFinance110',
        'dcid:CRSCooperationModalityB01',
        'dcid:CRSPurposeCode11110',
        'dcid:Commitment',
        null,
        null,
        'dcid:Nominal',
        'financingProvider,financingRecipient'
    ),
    (
        '{DISBURSEMENT_STATVAR}',
        'dcid:FinancialFlow',
        'dcid:amount',
        'dcid:measuredValue',
        'nettingBasis,financialFlowType,crsTypeOfFlow,crsTypeOfFinance,crsCooperationModality,crsPurpose',
        'dcid:CRSTypeOfFlow10',
        'dcid:CRSTypeOfFinance110',
        'dcid:CRSCooperationModalityB01',
        'dcid:CRSPurposeCode11110',
        null,
        'dcid:Disbursement',
        'dcid:Gross',
        'dcid:Nominal',
        'financingProvider,financingRecipient'
    )
) as statvars (
    dcid,
    population_type,
    measured_property,
    stat_type,
    constraint_properties,
    type_of_flow,
    type_of_finance,
    cooperation_modality,
    purpose_code,
    financial_obligation_type,
    financial_flow_type,
    netting_basis,
    measurement_qualifier,
    observation_properties
)
"""

OBSERVATIONS_SQL = f"""
create or replace table oecd_dac_crs.dc_dac_crs_observations as
select
    variable,
    date::integer as date,
    financing_provider,
    financing_recipient,
    value::decimal(18, 5) as value,
    unit
from (values
    ('{COMMITMENT_STATVAR}', 1973, 'country/FRA', 'country/KEN', 1.5, 'USDollar'),
    ('{COMMITMENT_STATVAR}', 1974, 'country/FRA', 'country/KEN', 2.5, 'USDollar'),
    ('{DISBURSEMENT_STATVAR}', 1974, 'AdaptationFund', 'country/KEN', 3.5, 'Euro')
) as observations (
    variable, date, financing_provider, financing_recipient, value, unit
)
"""


def create_dc_models(duckdb: DuckDBResource) -> None:
    """Create the DC schema models the export reads, holding a small fixture bundle"""
    with duckdb.get_connection() as connection:
        connection.execute("create schema if not exists oecd_dac_crs")
        connection.execute(PROVENANCE_SQL)
        connection.execute(STATVARS_SQL)
        connection.execute(OBSERVATIONS_SQL)


def run_export(tmp_path: Path) -> Path:
    """Materialize the export asset, returning the bundle directory"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "processed"))
    create_dc_models(duckdb)

    with patch("dfo_pipelines.orchestration.resources.date") as mock_date:
        mock_date.today.return_value = date(2026, 8, 3)
        out_dir = file_store.build_date_partitioned_path("oecd_dac_crs")
        oecd_dac_crs_dc_export(
            context=dg.build_asset_context(),
            duckdb=duckdb,
            export_file_store=file_store,
        )

    return Path(str(out_dir))


def test_dac_crs_dc_export_writes_the_import_config(tmp_path: Path) -> None:
    """Test the bundle's config registers its files, columns and observation period"""
    out_dir = run_export(tmp_path)

    assert json.loads((out_dir / "config.json").read_text()) == {
        "importName": "oecd_dac_crs",
        "inputFiles": [
            {
                "pattern": "*.mcf",
                "provenance": "dcid:provenance/OECDCreditorReportingSystem",
            },
            {
                "pattern": "observations_*.csv",
                "provenance": "dcid:provenance/OECDCreditorReportingSystem",
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


def test_dac_crs_dc_export_writes_the_conversion_recipe_onto_the_provenance_node(
    tmp_path: Path,
) -> None:
    """Test the Provenance node carries the spec's conversion recipe as a conversionRecipe arc"""
    out_dir = run_export(tmp_path)

    provenance = (out_dir / "provenance.mcf").read_text()
    assert "conversionRecipe: dcid:ConversionRecipeDAC\n" in provenance


def test_dac_crs_dc_export_writes_the_partial_year_onto_the_provenance_node(
    tmp_path: Path,
) -> None:
    """Test the Provenance node carries the partial year as partialObservationDate"""
    out_dir = run_export(tmp_path)

    provenance = (out_dir / "provenance.mcf").read_text()
    assert 'partialObservationDate: "1974"\n' in provenance


def test_dac_crs_dc_export_maps_the_crs_dimensions_to_ontology_properties(
    tmp_path: Path,
) -> None:
    """Test DAC CRS export asset names each dimension column as its ontology property"""
    out_dir = run_export(tmp_path)

    nodes = {
        node.splitlines()[0].removeprefix("Node: "): node.strip()
        for node in (out_dir / "custom_nodes.mcf").read_text().split("\n\n")
        if node.startswith("Node: ")
    }

    assert nodes[COMMITMENT_STATVAR] == (
        f"Node: {COMMITMENT_STATVAR}\n"
        "typeOf: dcid:StatisticalVariable\n"
        "statType: dcid:measuredValue\n"
        "populationType: dcid:FinancialObligation\n"
        "measuredProperty: dcid:amount\n"
        "constraintProperties: dcid:financialObligationType, dcid:crsTypeOfFlow, dcid:crsTypeOfFinance, dcid:crsCooperationModality, dcid:crsPurpose\n"
        "measurementQualifier: dcid:Nominal\n"
        "observationProperties: dcid:financingProvider, dcid:financingRecipient\n"
        "crsTypeOfFlow: dcid:CRSTypeOfFlow10\n"
        "crsTypeOfFinance: dcid:CRSTypeOfFinance110\n"
        "crsCooperationModality: dcid:CRSCooperationModalityB01\n"
        "crsPurpose: dcid:CRSPurposeCode11110\n"
        "financialObligationType: dcid:Commitment"
    )
    assert nodes[DISBURSEMENT_STATVAR] == (
        f"Node: {DISBURSEMENT_STATVAR}\n"
        "typeOf: dcid:StatisticalVariable\n"
        "statType: dcid:measuredValue\n"
        "populationType: dcid:FinancialFlow\n"
        "measuredProperty: dcid:amount\n"
        "constraintProperties: dcid:nettingBasis, dcid:financialFlowType, dcid:crsTypeOfFlow, dcid:crsTypeOfFinance, dcid:crsCooperationModality, dcid:crsPurpose\n"
        "measurementQualifier: dcid:Nominal\n"
        "observationProperties: dcid:financingProvider, dcid:financingRecipient\n"
        "crsTypeOfFlow: dcid:CRSTypeOfFlow10\n"
        "crsTypeOfFinance: dcid:CRSTypeOfFinance110\n"
        "crsCooperationModality: dcid:CRSCooperationModalityB01\n"
        "crsPurpose: dcid:CRSPurposeCode11110\n"
        "financialFlowType: dcid:Disbursement\n"
        "nettingBasis: dcid:Gross"
    )
