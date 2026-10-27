import json
import shutil
from contextlib import contextmanager
from datetime import UTC, date, datetime
from functools import partial
from pathlib import Path
from unittest.mock import patch

import dagster as dg
import pytest
import requests
from dagster_duckdb import DuckDBResource
from oda_reader.download.download_tools import DAC1_FLOW_URL
from upath import UPath

from dfo_pipelines.defs.oecd.dac1.assets import (
    OECD_DAC1_EXPORT_SPEC,
    oecd_dac1_dc_export,
    oecd_dac1_dc_ingestion,
    oecd_dac1_raw_file,
    oecd_dac1_raw_metadata,
    oecd_dac1_raw_table,
    oecd_dac1_source,
)
from dfo_pipelines.defs.resources import (
    KnowledgeGraphResource,
    UPathFileStoreResource,
)
from dfo_pipelines.extraction.metadata import (
    METADATA_FILE_NAME,
    read_extraction_metadata,
    write_extraction_metadata,
)
from tests.helpers import (
    AutomationHarness,
    column_list,
    list_parquet_columns,
    row_count,
)

FIXTURES = Path(__file__).parent / "fixtures"

# Stand-in for the resolved bulk-download URL: never fetched, the lookup is
# mocked, but joined into the data version so it is pinned in assertions.
DAC1_TEST_URL = "https://webfs-dcd.oecd.org/data/dac1/DAC1.parquet"

DOWNLOADED_AT = datetime(2026, 8, 3, 23, 47, 5, tzinfo=UTC)

SOURCE_RELEASE_DATE = date(2026, 7, 7)


def write_download(
    parquet: Path, save_to_path: str, use_raw_cache: bool = True
) -> None:
    """Write the given fixture to bulk_download_dac1 path"""
    shutil.copyfile(parquet, Path(save_to_path) / "table1_data.parquet")


@contextmanager
def patch_download_clock():
    """Freeze the instant the raw file asset stamps a download with"""
    with patch("dfo_pipelines.defs.oecd.dac1.assets.datetime") as mock_datetime:
        mock_datetime.now.return_value = DOWNLOADED_AT
        yield


@contextmanager
def patch_release_lookup():
    """Answer the version and release date lookups the raw file asset makes"""
    with (
        patch(
            "dfo_pipelines.defs.oecd.dac1.assets.get_bulk_file_url_with_version",
            autospec=True,
            return_value=(DAC1_TEST_URL, "0x8DEDC2882EAC20B"),
        ),
        patch(
            "dfo_pipelines.defs.oecd.dac1.assets.fetch_bulk_file_release_date",
            autospec=True,
            return_value=SOURCE_RELEASE_DATE,
        ) as release_lookup,
    ):
        yield release_lookup


def test_dac1_source_observe_fn_returns_metadata_and_data_version() -> None:
    """Test the source's observe fn records the URL, version and data version"""
    observe = oecd_dac1_source.observe_fn
    assert observe is not None

    with patch(
        "dfo_pipelines.defs.oecd.dac1.assets.get_bulk_file_url_with_version",
        autospec=True,
        return_value=(DAC1_TEST_URL, "v20260801"),
    ) as lookup:
        result = observe()

    # the full DAC1 parquet with a cache-bypassing lookup
    lookup.assert_called_once_with(
        flow_url=DAC1_FLOW_URL,
        label="DAC1 full dataset (dotStat format)",
        bypass_annotation_cache=True,
    )

    assert isinstance(result, dg.ObserveResult)
    assert result.data_version == dg.DataVersion(f"{DAC1_TEST_URL};v20260801")
    metadata = result.metadata
    assert metadata is not None
    uri_meta = metadata["dagster/uri"]
    version_meta = metadata["version"]
    assert isinstance(uri_meta, dg.TextMetadataValue)
    assert isinstance(version_meta, dg.TextMetadataValue)
    assert uri_meta.value == DAC1_TEST_URL
    assert version_meta.value == "v20260801"
    assert "url" not in metadata


def test_dac1_source_observe_fn_raises_when_version_unavailable() -> None:
    """Test the observe fn raises when the lookup returns no version token

    Without a token there is nothing to gate the pipeline on, so the
    observation must fail loudly rather than record an empty data version.
    """
    observe = oecd_dac1_source.observe_fn
    assert observe is not None

    with (
        patch(
            "dfo_pipelines.defs.oecd.dac1.assets.get_bulk_file_url_with_version",
            autospec=True,
            return_value=(DAC1_TEST_URL, None),
        ),
        pytest.raises(ValueError, match="DAC1 version check"),
    ):
        observe()


def test_dac1_source_observe_fn_raises_when_lookup_fails() -> None:
    """Test an unreachable OECD API surfaces rather than being skipped"""
    observe = oecd_dac1_source.observe_fn
    assert observe is not None

    with (
        patch(
            "dfo_pipelines.defs.oecd.dac1.assets.get_bulk_file_url_with_version",
            autospec=True,
            side_effect=requests.exceptions.ConnectionError("sdmx unreachable"),
        ),
        pytest.raises(requests.exceptions.ConnectionError, match="sdmx unreachable"),
    ):
        observe()


class _DAC1Automation(AutomationHarness):
    """Drives the DAC1 assets against an ephemeral instance, as the daemon does

    Version lookups and downloads are mocked for as long as the context is
    open. Assign to `version` to make the next observation see a republished
    source.
    """

    def __init__(self, tmp_path: Path) -> None:
        super().__init__(
            tmp_path,
            source=oecd_dac1_source,
            raw_assets=[oecd_dac1_raw_file, oecd_dac1_raw_table],
        )
        self.version = "v20260801"

    def _start_mocks(self) -> None:
        self._stack.enter_context(
            patch(
                "dfo_pipelines.defs.oecd.dac1.assets.get_bulk_file_url_with_version",
                autospec=True,
                side_effect=lambda **kwargs: (DAC1_TEST_URL, self.version),
            )
        )
        self._stack.enter_context(
            patch(
                "dfo_pipelines.defs.oecd.dac1.assets.fetch_bulk_file_release_date",
                autospec=True,
                return_value=SOURCE_RELEASE_DATE,
            )
        )
        self._stack.enter_context(
            patch(
                "dfo_pipelines.defs.oecd.dac1.assets.bulk_download_dac1",
                side_effect=partial(write_download, FIXTURES / "data.parquet"),
            )
        )


def test_dac1_source_observation_records_an_observation_event(
    tmp_path: Path,
) -> None:
    """Test observing the source records one observation against the source"""

    with _DAC1Automation(tmp_path) as automation:
        result = automation.observe()

    assert [
        entry.asset_key
        for entry in result.all_events
        if entry.event_type_value == "ASSET_OBSERVATION"
    ] == [oecd_dac1_source.key]


def test_dac1_raw_assets_are_not_rerun_when_the_source_is_unchanged(
    tmp_path: Path,
) -> None:
    """Test a repeat observation at an unchanged data version requests no rerun

    This is the point of the new data check: the raw assets must not re-run
    while the OECD has not republished the bulk file.
    """
    with _DAC1Automation(tmp_path) as automation:
        automation.observe()
        automation.materialize(oecd_dac1_raw_file, oecd_dac1_raw_table)
        # A baseline tick, not evidence: every condition is reset on its initial
        # evaluation, so the first tick requests nothing whatever the source did.
        automation.tick()

        automation.observe()

        assert automation.tick() == {
            "oecd_dac1_raw_file": 0,
            "oecd_dac1_raw_table": 0,
        }


def test_dac1_source_is_requested_once_per_daily_cron_tick(tmp_path: Path) -> None:
    """Test the source's condition requests one observation per UTC day

    The 13:00 tick is what pins the cadence: a shorter cron would request an
    observation an hour after the last one. The 00:01 tick proves the condition
    requests an observation at all once the daily tick has passed, and 00:02
    proves it is not requested again before the next one.
    """
    with _DAC1Automation(tmp_path) as automation:
        # A baseline tick: every condition is reset on its initial evaluation,
        # so the first tick requests nothing whatever the time is.
        automation.source_tick(at=datetime(2026, 8, 25, 12, 0, tzinfo=UTC))

        assert automation.source_tick(at=datetime(2026, 8, 25, 13, 0, tzinfo=UTC)) == 0
        assert automation.source_tick(at=datetime(2026, 8, 26, 0, 1, tzinfo=UTC)) == 1
        assert automation.source_tick(at=datetime(2026, 8, 26, 0, 2, tzinfo=UTC)) == 0


def test_dac1_raw_assets_are_rerun_when_the_source_data_version_changes(
    tmp_path: Path,
) -> None:
    """Test a changed data version requests the raw assets, and only once"""
    with _DAC1Automation(tmp_path) as automation:
        automation.observe()
        automation.materialize(oecd_dac1_raw_file, oecd_dac1_raw_table)
        automation.tick()  # baseline, as above

        automation.version = "v20260901"
        automation.observe()

        # Both are requested on the same tick: eager() counts a dependency that
        # will be requested as updated, so the chain is launched in one run.
        assert automation.tick() == {
            "oecd_dac1_raw_file": 1,
            "oecd_dac1_raw_table": 1,
        }

        automation.materialize(oecd_dac1_raw_file, oecd_dac1_raw_table)

        assert automation.tick() == {
            "oecd_dac1_raw_file": 0,
            "oecd_dac1_raw_table": 0,
        }


def test_dac1_raw_file_writes_parquet(tmp_path: Path) -> None:
    """Test the raw file asset writes the download, and the metadata beside it

    Both are stamped from one clock reading, so the partition a file lands in
    and the date the provenance publishes for it can never disagree.
    """
    observe = oecd_dac1_source.observe_fn
    assert observe is not None
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
    raw_dir = file_store.build_date_partitioned_path(
        "oecd_dac1/dac1", partition_date=DOWNLOADED_AT.date()
    )

    with (
        patch_download_clock(),
        patch_release_lookup() as release_lookup,
        patch(
            "dfo_pipelines.defs.oecd.dac1.assets.bulk_download_dac1",
            side_effect=partial(write_download, FIXTURES / "data.parquet"),
        ) as bulk_download,
    ):
        result = oecd_dac1_raw_file(
            context=dg.build_asset_context(), raw_file_store=file_store
        )
        observation = observe()

    bulk_download.assert_called_once_with(
        save_to_path=str(raw_dir), use_raw_cache=False
    )
    release_lookup.assert_called_once_with(DAC1_TEST_URL)

    recorded = read_extraction_metadata(raw_dir, release_names=("source",))
    assert recorded.downloaded_at == DOWNLOADED_AT
    assert recorded.release_dates == {"source": SOURCE_RELEASE_DATE}
    metadata_json = json.loads((raw_dir / METADATA_FILE_NAME).read_text())
    assert set(metadata_json) == {"downloaded_at", "source_release_date"}

    assert list_parquet_columns(Path(str(raw_dir)) / "data.parquet") == [
        "DONOR",
        "Donor",
        "PART",
        "Part",
        "AIDTYPE",
        "Aid type",
        "FLOWS",
        "Fund flows",
        "AMOUNTTYPE",
        "Amount type",
        "TIME",
        "Year",
        "Value",
        "Flags",
    ]

    assert isinstance(observation, dg.ObserveResult)
    assert observation.metadata is not None
    observed_version = observation.metadata["version"]
    assert isinstance(result, dg.MaterializeResult)
    assert result.value == str(raw_dir / "data.parquet")
    assert result.metadata == {
        "dagster/uri": dg.TextMetadataValue(str(raw_dir / "data.parquet")),
        "file_size": dg.IntMetadataValue((FIXTURES / "data.parquet").stat().st_size),
        "version": observed_version,
        "dagster/row_count": dg.IntMetadataValue(25),
    }
    [check] = result.check_results
    assert check.check_name == "file_not_empty"
    assert check.passed


def test_dac1_raw_file_check_fails_when_file_empty(tmp_path: Path) -> None:
    """Test DAC1 raw file check fails when the download is empty"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))

    with (
        patch_release_lookup(),
        patch(
            "dfo_pipelines.defs.oecd.dac1.assets.bulk_download_dac1",
            side_effect=partial(write_download, FIXTURES / "empty.parquet"),
        ),
    ):
        result = oecd_dac1_raw_file(
            context=dg.build_asset_context(), raw_file_store=file_store
        )

    assert isinstance(result, dg.MaterializeResult)

    [check] = result.check_results

    assert check.check_name == "file_not_empty"
    assert not check.passed


def test_dac1_raw_table_loads_parquet(tmp_path: Path) -> None:
    """Test DAC1 raw table asset loads the parquet file into the database"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    result = oecd_dac1_raw_table(
        duckdb=duckdb, oecd_dac1_raw_file=str(FIXTURES / "data.parquet")
    )

    assert row_count(duckdb, "oecd_dac1.raw_dac1") == 25
    assert column_list(duckdb, "oecd_dac1.raw_dac1") == [
        "donor_code",
        "donor_name",
        "part_code",
        "part_name",
        "AIDTYPE",
        "Aid type",
        "FLOWS",
        "Fund flows",
        "AMOUNTTYPE",
        "Amount type",
        "TIME",
        "Year",
        "Value",
        "Flags",
    ]
    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata == {
        "dagster/table_name": dg.TextMetadataValue("oecd_dac1.raw_dac1"),
        "dagster/row_count": dg.IntMetadataValue(25),
    }
    [check] = result.check_results
    assert check.check_name == "table_not_empty"
    assert check.passed


def test_dac1_raw_table_check_fails_when_table_empty(tmp_path: Path) -> None:
    """Test DAC1 raw table check fails when the parquet file has no rows"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    result = oecd_dac1_raw_table(
        duckdb=duckdb, oecd_dac1_raw_file=str(FIXTURES / "empty.parquet")
    )

    assert row_count(duckdb, "oecd_dac1.raw_dac1") == 0

    assert isinstance(result, dg.MaterializeResult)

    [check] = result.check_results

    assert check.check_name == "table_not_empty"
    assert not check.passed


def test_dac1_raw_table_overwrites_existing_table(tmp_path: Path) -> None:
    """Test DAC1 raw table asset replaces the table rather than appending"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    oecd_dac1_raw_table(
        duckdb=duckdb, oecd_dac1_raw_file=str(FIXTURES / "data.parquet")
    )

    assert row_count(duckdb, "oecd_dac1.raw_dac1") == 25

    oecd_dac1_raw_table(
        duckdb=duckdb, oecd_dac1_raw_file=str(FIXTURES / "data-5.parquet")
    )

    assert row_count(duckdb, "oecd_dac1.raw_dac1") == 5


def test_dac1_raw_metadata_loads_the_recorded_metadata(tmp_path: Path) -> None:
    """Test the metadata asset loads the extraction metadata with the raw file"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    write_extraction_metadata(
        UPath(tmp_path),
        downloaded_at=DOWNLOADED_AT,
        release_dates={"source": SOURCE_RELEASE_DATE},
    )

    result = oecd_dac1_raw_metadata(
        duckdb=duckdb, oecd_dac1_raw_file=str(tmp_path / "data.parquet")
    )

    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata == {
        "dagster/table_name": dg.TextMetadataValue("oecd_dac1.raw_dac1_metadata"),
        "dagster/row_count": dg.IntMetadataValue(1),
    }


def test_dac1_raw_file_path_reaches_raw_table(tmp_path: Path) -> None:
    """Test the raw file asset's output path is injected into the downstream assets"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    with (
        patch_release_lookup(),
        patch(
            "dfo_pipelines.defs.oecd.dac1.assets.bulk_download_dac1",
            side_effect=partial(write_download, FIXTURES / "data.parquet"),
        ),
    ):
        result = dg.materialize(
            [oecd_dac1_raw_file, oecd_dac1_raw_table, oecd_dac1_raw_metadata],
            resources={"raw_file_store": file_store, "duckdb": duckdb},
        )

    assert result.success

    checks = {e.check_name: e.passed for e in result.get_asset_check_evaluations()}

    assert checks == {"file_not_empty": True, "table_not_empty": True}
    assert row_count(duckdb, "oecd_dac1.raw_dac1") == 25
    assert row_count(duckdb, "oecd_dac1.raw_dac1_metadata") == 1


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


def _run_dc_export(tmp_path: Path) -> tuple[dg.MaterializeResult, Path]:
    """Materialize the export over a small fixture bundle"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    with duckdb.get_connection() as connection:
        connection.execute(DC_MODELS_SQL)
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "processed"))

    with patch("dfo_pipelines.defs.resources.date") as mock_date:
        mock_date.today.return_value = date(2026, 10, 2)
        result = oecd_dac1_dc_export(
            context=dg.build_asset_context(),
            duckdb=duckdb,
            export_file_store=file_store,
        )

    assert isinstance(result, dg.MaterializeResult)
    return result, tmp_path / "processed" / "oecd_dac1" / "2026" / "10" / "02"


def test_oecd_dac1_dc_export_writes_a_bundle_to_a_date_partitioned_path(
    tmp_path: Path,
) -> None:
    """Test the export reports the bundle it wrote to today's partition"""
    result, out_dir = _run_dc_export(tmp_path)

    assert result.value == str(out_dir)
    assert result.metadata == {
        "dagster/uri": dg.TextMetadataValue(str(out_dir)),
        "observation_count": dg.IntMetadataValue(4),
        "statvar_count": dg.IntMetadataValue(2),
        "observation_file_count": dg.IntMetadataValue(2),
    }


def test_oecd_dac1_dc_export_writes_the_import_config(tmp_path: Path) -> None:
    """Test the config registers flows by provider and recipient, and GNI and
    population about the provider, each with the period
    """
    _, out_dir = _run_dc_export(tmp_path)

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
    _, out_dir = _run_dc_export(tmp_path)

    provenance = (out_dir / "provenance.mcf").read_text()
    assert "conversionRecipe: dcid:ConversionRecipeDAC\n" in provenance


def test_oecd_dac1_dc_export_writes_each_property_the_statvar_sets(
    tmp_path: Path,
) -> None:
    """Test a StatVar's constraints, denominator, entities and aid type code become
    its properties
    """
    _, out_dir = _run_dc_export(tmp_path)

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


def test_oecd_dac1_ingestion_ingests_the_bundle_the_export_wrote(
    tmp_path: Path, knowledge_graph: KnowledgeGraphResource
) -> None:
    """Test the ingestion is handed the export's directory under its import name"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    with duckdb.get_connection() as connection:
        connection.execute(DC_MODELS_SQL)
    import_name = OECD_DAC1_EXPORT_SPEC.import_name
    destination = f"gs://test-bucket/ingestion/input/{import_name}"

    with patch(
        "dfo_pipelines.defs.oecd.dac1.assets.ingest_bundle",
        return_value=destination,
    ) as ingest_bundle:
        result = dg.materialize(
            [oecd_dac1_dc_export, oecd_dac1_dc_ingestion],
            resources={
                "duckdb": duckdb,
                "export_file_store": UPathFileStoreResource(
                    base_path=str(tmp_path / "processed")
                ),
                "knowledge_graph": knowledge_graph,
            },
        )

    assert result.success
    [export] = result.asset_materializations_for_node("oecd_dac1_dc_export")
    [ingestion] = result.asset_materializations_for_node("oecd_dac1_dc_ingestion")
    bundle = export.metadata["dagster/uri"].value
    assert "import_name" not in export.metadata
    ingest_bundle.assert_called_once()
    args, kwargs = ingest_bundle.call_args
    assert str(args[0]) == bundle
    assert kwargs["import_name"] == import_name
    assert kwargs["settings"] == knowledge_graph.settings
    assert ingestion.metadata["dagster/uri"].value == destination
    assert "import_name" not in ingestion.metadata
