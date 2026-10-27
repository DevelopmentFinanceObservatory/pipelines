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
from oda_reader.download.download_tools import CRS_FLOW_URL
from upath import UPath

from dfo_pipelines.defs.oecd.dac_crs.assets import (
    CRS_EXPORT_SPEC,
    oecd_dac_crs_dc_export,
    oecd_dac_crs_dc_ingestion,
    oecd_dac_crs_raw_file,
    oecd_dac_crs_raw_metadata,
    oecd_dac_crs_raw_table,
    oecd_dac_crs_source,
    parse_crs_release_date,
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
from tests.helpers import AutomationHarness, row_count

FIXTURES = Path(__file__).parent / "fixtures"

# Stand-in for the resolved bulk-download URL: never fetched, the lookup is
# mocked, but joined into the data version so it is pinned in assertions.
CRS_TEST_URL = "https://webfs-dcd.oecd.org/data/dac_crs/CRS.parquet"

DOWNLOADED_AT = datetime(2026, 8, 3, 23, 47, 5, tzinfo=UTC)

SOURCE_VERSION = "v20260801"
SOURCE_RELEASE_DATE = date(2026, 8, 1)


def write_download(parquet: Path, save_to_path: str, **kwargs) -> None:
    """Write the given fixture to bulk_download_crs path"""
    shutil.copyfile(parquet, Path(save_to_path) / "CRS.parquet")


@contextmanager
def patch_download_clock():
    """Freeze the instant the raw file asset stamps a download with"""
    with patch("dfo_pipelines.defs.oecd.dac_crs.assets.datetime") as mock_datetime:
        mock_datetime.now.return_value = DOWNLOADED_AT
        yield


@contextmanager
def patch_bulk_lookup():
    """Answer the version lookup the raw file asset makes before downloading"""
    with patch(
        "dfo_pipelines.defs.oecd.dac_crs.assets.get_bulk_file_url_with_version",
        autospec=True,
        return_value=(CRS_TEST_URL, SOURCE_VERSION),
    ) as lookup:
        yield lookup


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


def test_dac_crs_source_observe_fn_returns_metadata_and_data_version() -> None:
    """Test the source's observe fn records the URL, version and data version"""
    observe = oecd_dac_crs_source.observe_fn
    assert observe is not None

    with patch_bulk_lookup() as lookup:
        result = observe()

    # the full CRS parquet with a cache-bypassing lookup
    lookup.assert_called_once_with(
        flow_url=CRS_FLOW_URL, label="CRS-Parquet", bypass_annotation_cache=True
    )

    assert isinstance(result, dg.ObserveResult)
    assert result.data_version == dg.DataVersion(f"{CRS_TEST_URL};{SOURCE_VERSION}")
    metadata = result.metadata
    assert metadata is not None
    uri_meta = metadata["dagster/uri"]
    version_meta = metadata["version"]
    assert isinstance(uri_meta, dg.TextMetadataValue)
    assert isinstance(version_meta, dg.TextMetadataValue)
    assert uri_meta.value == CRS_TEST_URL
    assert version_meta.value == SOURCE_VERSION
    assert "url" not in metadata


def test_dac_crs_source_observe_fn_raises_when_version_unavailable() -> None:
    """Test the observe fn raises when the lookup returns no version token

    Without a token there is nothing to gate the pipeline on, so the
    observation must fail loudly rather than record an empty data version.
    """
    observe = oecd_dac_crs_source.observe_fn
    assert observe is not None

    with (
        patch(
            "dfo_pipelines.defs.oecd.dac_crs.assets.get_bulk_file_url_with_version",
            autospec=True,
            return_value=(CRS_TEST_URL, None),
        ),
        pytest.raises(ValueError, match="CRS version check"),
    ):
        observe()


def test_dac_crs_source_observe_fn_raises_when_lookup_fails() -> None:
    """Test an unreachable OECD API surfaces rather than being skipped"""
    observe = oecd_dac_crs_source.observe_fn
    assert observe is not None

    with (
        patch(
            "dfo_pipelines.defs.oecd.dac_crs.assets.get_bulk_file_url_with_version",
            autospec=True,
            side_effect=requests.exceptions.ConnectionError("sdmx unreachable"),
        ),
        pytest.raises(requests.exceptions.ConnectionError, match="sdmx unreachable"),
    ):
        observe()


class _CRSAutomation(AutomationHarness):
    """Drives the CRS assets against an ephemeral instance, as the daemon does

    Version lookups and downloads are mocked for as long as the context is
    open. Assign to `version` to make the next observation see a republished
    source.
    """

    def __init__(self, tmp_path: Path) -> None:
        super().__init__(
            tmp_path,
            source=oecd_dac_crs_source,
            raw_assets=[
                oecd_dac_crs_raw_file,
                oecd_dac_crs_raw_table,
                oecd_dac_crs_raw_metadata,
            ],
        )
        self.version = SOURCE_VERSION

    def _start_mocks(self) -> None:
        self._stack.enter_context(
            patch(
                "dfo_pipelines.defs.oecd.dac_crs.assets.get_bulk_file_url_with_version",
                autospec=True,
                side_effect=lambda **kwargs: (CRS_TEST_URL, self.version),
            )
        )
        self._stack.enter_context(
            patch(
                "dfo_pipelines.defs.oecd.dac_crs.assets.bulk_download_crs",
                side_effect=partial(write_download, FIXTURES / "data.parquet"),
            )
        )


def test_dac_crs_source_observation_records_an_observation_event(
    tmp_path: Path,
) -> None:
    """Test observing the source records one observation against the source"""

    with _CRSAutomation(tmp_path) as automation:
        result = automation.observe()

    assert [
        entry.asset_key
        for entry in result.all_events
        if entry.event_type_value == "ASSET_OBSERVATION"
    ] == [oecd_dac_crs_source.key]


def test_dac_crs_raw_assets_are_not_rerun_when_the_source_is_unchanged(
    tmp_path: Path,
) -> None:
    """Test a repeat observation at an unchanged data version requests no rerun

    This is the point of the new data check: the raw assets must not re-run
    while the OECD has not republished the bulk file.
    """
    with _CRSAutomation(tmp_path) as automation:
        automation.observe()
        automation.materialize(
            oecd_dac_crs_raw_file, oecd_dac_crs_raw_table, oecd_dac_crs_raw_metadata
        )
        # A baseline tick, not evidence: every condition is reset on its initial
        # evaluation, so the first tick requests nothing whatever the source did.
        automation.tick()

        automation.observe()

        assert automation.tick() == {
            "oecd_dac_crs_raw_file": 0,
            "oecd_dac_crs_raw_table": 0,
            "oecd_dac_crs_raw_metadata": 0,
        }


def test_dac_crs_source_is_requested_once_per_daily_cron_tick(tmp_path: Path) -> None:
    """Test the source's condition requests one observation per UTC day

    The 13:00 tick is what pins the cadence: a shorter cron would request an
    observation an hour after the last one. The 00:01 tick proves the condition
    requests an observation at all once the daily tick has passed, and 00:02
    proves it is not requested again before the next one.
    """
    with _CRSAutomation(tmp_path) as automation:
        # A baseline tick: every condition is reset on its initial evaluation,
        # so the first tick requests nothing whatever the time is.
        automation.source_tick(at=datetime(2026, 8, 25, 12, 0, tzinfo=UTC))

        assert automation.source_tick(at=datetime(2026, 8, 25, 13, 0, tzinfo=UTC)) == 0
        assert automation.source_tick(at=datetime(2026, 8, 26, 0, 1, tzinfo=UTC)) == 1
        assert automation.source_tick(at=datetime(2026, 8, 26, 0, 2, tzinfo=UTC)) == 0


def test_dac_crs_raw_assets_are_rerun_when_the_source_data_version_changes(
    tmp_path: Path,
) -> None:
    """Test a changed data version requests the raw assets, and only once"""
    with _CRSAutomation(tmp_path) as automation:
        automation.observe()
        automation.materialize(
            oecd_dac_crs_raw_file, oecd_dac_crs_raw_table, oecd_dac_crs_raw_metadata
        )
        automation.tick()  # baseline, as above

        automation.version = "v20260901"
        automation.observe()

        # Both are requested on the same tick: eager() counts a dependency that
        # will be requested as updated, so the chain is launched in one run.
        assert automation.tick() == {
            "oecd_dac_crs_raw_file": 1,
            "oecd_dac_crs_raw_table": 1,
            "oecd_dac_crs_raw_metadata": 1,
        }

        automation.materialize(
            oecd_dac_crs_raw_file, oecd_dac_crs_raw_table, oecd_dac_crs_raw_metadata
        )

        assert automation.tick() == {
            "oecd_dac_crs_raw_file": 0,
            "oecd_dac_crs_raw_table": 0,
            "oecd_dac_crs_raw_metadata": 0,
        }


def test_dac_crs_raw_file_writes_parquet(tmp_path: Path) -> None:
    """Test the raw file asset writes the download, and the metadata beside it

    Both are stamped from one clock reading, so the partition a file lands in
    and the date the provenance publishes for it can never disagree - not
    across midnight, and not across timezones.
    """
    observe = oecd_dac_crs_source.observe_fn
    assert observe is not None
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))

    raw_dir = file_store.build_date_partitioned_path(
        "oecd_dac_crs/dac_crs", partition_date=DOWNLOADED_AT.date()
    )

    with (
        patch_download_clock(),
        patch_bulk_lookup(),
        patch(
            "dfo_pipelines.defs.oecd.dac_crs.assets.bulk_download_crs",
            side_effect=partial(write_download, FIXTURES / "data.parquet"),
        ) as bulk_download,
    ):
        result = oecd_dac_crs_raw_file(
            context=dg.build_asset_context(), raw_file_store=file_store
        )
        observation = observe()

    bulk_download.assert_called_once()
    assert bulk_download.call_args.kwargs["save_to_path"] == str(raw_dir)

    recorded = read_extraction_metadata(raw_dir, release_names=("source",))
    downloaded_at = recorded.downloaded_at
    assert downloaded_at == DOWNLOADED_AT
    assert recorded.release_dates == {"source": SOURCE_RELEASE_DATE}
    assert raw_dir.parts[-3:] == tuple(downloaded_at.strftime("%Y/%m/%d").split("/"))

    metadata_json = json.loads((raw_dir / METADATA_FILE_NAME).read_text())
    assert set(metadata_json) == {"downloaded_at", "source_release_date"}

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


def test_dac_crs_raw_file_check_fails_when_file_empty(tmp_path: Path) -> None:
    """Test DAC CRS raw file check fails when the download has no rows"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))

    with (
        patch_bulk_lookup(),
        patch(
            "dfo_pipelines.defs.oecd.dac_crs.assets.bulk_download_crs",
            side_effect=partial(write_download, FIXTURES / "data-0.parquet"),
        ),
    ):
        result = oecd_dac_crs_raw_file(
            context=dg.build_asset_context(), raw_file_store=file_store
        )

    assert isinstance(result, dg.MaterializeResult)

    [check] = result.check_results

    assert check.check_name == "file_not_empty"
    assert not check.passed


def test_dac_crs_raw_file_raises_when_download_fails(tmp_path: Path) -> None:
    """Test a download error surfaces rather than being skipped"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))

    with (
        patch_bulk_lookup(),
        patch(
            "dfo_pipelines.defs.oecd.dac_crs.assets.bulk_download_crs",
            side_effect=requests.exceptions.ConnectionError("download failed"),
        ),
        pytest.raises(requests.exceptions.ConnectionError, match="download failed"),
    ):
        oecd_dac_crs_raw_file(
            context=dg.build_asset_context(), raw_file_store=file_store
        )


def test_dac_crs_raw_table_loads_parquet(tmp_path: Path) -> None:
    """Test DAC CRS raw table asset loads the parquet file into the database"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    result = oecd_dac_crs_raw_table(
        duckdb=duckdb, oecd_dac_crs_raw_file=str(FIXTURES / "data.parquet")
    )

    assert row_count(duckdb, "oecd_dac_crs.raw_dac_crs") == 25
    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata == {
        "dagster/table_name": dg.TextMetadataValue("oecd_dac_crs.raw_dac_crs"),
        "dagster/row_count": dg.IntMetadataValue(25),
    }
    [check] = result.check_results
    assert check.check_name == "table_not_empty"
    assert check.passed


def test_dac_crs_raw_table_check_fails_when_table_empty(tmp_path: Path) -> None:
    """Test DAC CRS raw table check fails when the parquet file has no rows"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    result = oecd_dac_crs_raw_table(
        duckdb=duckdb, oecd_dac_crs_raw_file=str(FIXTURES / "data-0.parquet")
    )

    assert row_count(duckdb, "oecd_dac_crs.raw_dac_crs") == 0

    assert isinstance(result, dg.MaterializeResult)

    [check] = result.check_results

    assert check.check_name == "table_not_empty"
    assert not check.passed


def test_dac_crs_raw_table_overwrites_existing_table(tmp_path: Path) -> None:
    """Test DAC CRS raw table asset replaces the table rather than appending"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    oecd_dac_crs_raw_table(
        duckdb=duckdb, oecd_dac_crs_raw_file=str(FIXTURES / "data.parquet")
    )

    assert row_count(duckdb, "oecd_dac_crs.raw_dac_crs") == 25

    oecd_dac_crs_raw_table(
        duckdb=duckdb, oecd_dac_crs_raw_file=str(FIXTURES / "data-5.parquet")
    )

    assert row_count(duckdb, "oecd_dac_crs.raw_dac_crs") == 5


def test_dac_crs_raw_metadata_loads_the_recorded_metadata(tmp_path: Path) -> None:
    """Test the metadata asset loads the extraction metadata with the raw file"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    write_extraction_metadata(
        UPath(tmp_path),
        downloaded_at=DOWNLOADED_AT,
        release_dates={"source": SOURCE_RELEASE_DATE},
    )

    result = oecd_dac_crs_raw_metadata(
        duckdb=duckdb, oecd_dac_crs_raw_file=str(tmp_path / "data.parquet")
    )

    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata == {
        "dagster/table_name": dg.TextMetadataValue("oecd_dac_crs.raw_dac_crs_metadata"),
        "dagster/row_count": dg.IntMetadataValue(1),
    }


def test_dac_crs_raw_file_path_reaches_raw_table(tmp_path: Path) -> None:
    """Test the raw file asset's output path is injected into the downstream assets"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    with (
        patch_bulk_lookup(),
        patch(
            "dfo_pipelines.defs.oecd.dac_crs.assets.bulk_download_crs",
            side_effect=partial(write_download, FIXTURES / "data.parquet"),
        ),
    ):
        result = dg.materialize(
            [oecd_dac_crs_raw_file, oecd_dac_crs_raw_table, oecd_dac_crs_raw_metadata],
            resources={"raw_file_store": file_store, "duckdb": duckdb},
        )

    assert result.success

    assert row_count(duckdb, "oecd_dac_crs.raw_dac_crs") == 25
    assert row_count(duckdb, "oecd_dac_crs.raw_dac_crs_metadata") == 1


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


def run_export(tmp_path: Path) -> tuple[dg.MaterializeResult, Path]:
    """Materialize the export asset, returning the result and the bundle directory"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "processed"))
    create_dc_models(duckdb)

    with patch("dfo_pipelines.defs.resources.date") as mock_date:
        mock_date.today.return_value = date(2026, 8, 3)
        out_dir = file_store.build_date_partitioned_path("oecd_dac_crs")
        result = oecd_dac_crs_dc_export(
            context=dg.build_asset_context(),
            duckdb=duckdb,
            export_file_store=file_store,
        )

    assert isinstance(result, dg.MaterializeResult)

    return result, Path(str(out_dir))


def test_dac_crs_dc_export_writes_a_bundle_to_a_date_partitioned_path(
    tmp_path: Path,
) -> None:
    """Test DAC CRS export asset reports the bundle it wrote to today's partition"""
    result, out_dir = run_export(tmp_path)

    assert out_dir == tmp_path / "processed" / "oecd_dac_crs" / "2026" / "08" / "03"
    assert result.metadata == {
        "dagster/uri": dg.TextMetadataValue(str(out_dir)),
        "observation_count": dg.IntMetadataValue(3),
        "statvar_count": dg.IntMetadataValue(2),
        "observation_file_count": dg.IntMetadataValue(2),
    }


def test_dac_crs_dc_export_writes_the_import_config(tmp_path: Path) -> None:
    """Test the bundle's config registers its files, columns and observation period"""
    _, out_dir = run_export(tmp_path)

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
    _, out_dir = run_export(tmp_path)

    provenance = (out_dir / "provenance.mcf").read_text()
    assert "conversionRecipe: dcid:ConversionRecipeDAC\n" in provenance


def test_dac_crs_dc_export_maps_the_crs_dimensions_to_ontology_properties(
    tmp_path: Path,
) -> None:
    """Test DAC CRS export asset names each dimension column as its ontology property"""
    _, out_dir = run_export(tmp_path)

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


def test_dac_crs_ingestion_ingests_the_bundle_the_export_wrote(
    tmp_path: Path, knowledge_graph: KnowledgeGraphResource
) -> None:
    """Test the ingestion is handed the export's directory under its import name"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    create_dc_models(duckdb)
    import_name = CRS_EXPORT_SPEC.import_name
    destination = f"gs://test-bucket/ingestion/input/{import_name}"

    with patch(
        "dfo_pipelines.defs.oecd.dac_crs.assets.ingest_bundle",
        return_value=destination,
    ) as ingest_bundle:
        result = dg.materialize(
            [oecd_dac_crs_dc_export, oecd_dac_crs_dc_ingestion],
            resources={
                "duckdb": duckdb,
                "export_file_store": UPathFileStoreResource(
                    base_path=str(tmp_path / "processed")
                ),
                "knowledge_graph": knowledge_graph,
            },
        )

    assert result.success
    [export] = result.asset_materializations_for_node("oecd_dac_crs_dc_export")
    [ingestion] = result.asset_materializations_for_node("oecd_dac_crs_dc_ingestion")
    bundle = export.metadata["dagster/uri"].value
    assert "import_name" not in export.metadata
    ingest_bundle.assert_called_once()
    args, kwargs = ingest_bundle.call_args
    assert str(args[0]) == bundle
    assert kwargs["import_name"] == import_name
    assert kwargs["settings"] == knowledge_graph.settings
    assert ingestion.metadata["dagster/uri"].value == destination
    assert "import_name" not in ingestion.metadata
