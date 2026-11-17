import json
from datetime import UTC, date, datetime
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
    fetch_weo_publication_date,
    fetch_weo_raw_file,
    imf_weo_dc_export,
    imf_weo_raw_file,
    imf_weo_raw_metadata,
    imf_weo_raw_table,
    imf_weo_source,
)
from dfo_pipelines.extraction.metadata import (
    write_extraction_metadata,
)
from dfo_pipelines.orchestration.resources import UPathFileStoreResource
from tests.helpers import (
    AutomationHarness,
    column_list,
    list_first_row,
    list_parquet_columns,
    list_parquet_first_row,
    row_count,
)

FIXTURES = Path(__file__).parent / "fixtures"
FIXTURE_ROWS = len(pd.read_parquet(FIXTURES / "data.parquet"))
DOWNLOADED_AT = datetime(2026, 9, 11, 9, 14, 22, tzinfo=UTC)
PUBLICATION_DATE = date(2026, 4, 14)


def return_dataframe(parquet: Path) -> pd.DataFrame:
    """Return dataframe read fixture from parquet file"""
    return pd.read_parquet(parquet)


def test_fetch_weo_raw_file_pins_the_download_to_the_resolved_release(
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
        fetched = fetch_weo_raw_file(file_path)

    fetch.assert_called_once_with(version=("April", 2026))
    publication_date.assert_called_once_with(("April", 2026))
    assert len(pd.read_parquet(file_path)) == FIXTURE_ROWS
    assert fetched.release_dates == {"source": PUBLICATION_DATE}
    assert fetched.metadata == {"version": dg.TextMetadataValue("April 2026")}


def test_fetch_weo_raw_file_raises_when_the_release_cannot_be_served(
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
        fetch_weo_raw_file(file_path)

    assert not file_path.exists()


def test_imf_weo_raw_table_loads_parquet(tmp_path: Path) -> None:
    """Test IMF WEO raw table asset loads the parquet file into the database"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    result = imf_weo_raw_table(duckdb=duckdb, raw_file=str(FIXTURES / "data.parquet"))

    assert row_count(duckdb, "imf_weo.raw_weo") == FIXTURE_ROWS
    assert column_list(duckdb, "imf_weo.raw_weo") == list_parquet_columns(
        FIXTURES / "data.parquet"
    )
    assert list_first_row(duckdb, "imf_weo.raw_weo") == list_parquet_first_row(
        FIXTURES / "data.parquet"
    )
    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata == {
        "dagster/table_name": dg.TextMetadataValue("imf_weo.raw_weo"),
        "dagster/row_count": dg.IntMetadataValue(FIXTURE_ROWS),
    }
    [check] = result.check_results
    assert check.check_name == "table_not_empty"
    assert check.passed


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


class _WEOAutomation(AutomationHarness):
    """Drives the WEO assets against an ephemeral instance, as the daemon does

    get_weo_versions is mocked for as long as the context is open. Assign to
    `release` to make the next observation see a newly published release.
    """

    def __init__(self, tmp_path: Path) -> None:
        super().__init__(
            tmp_path,
            source=imf_weo_source,
            raw_assets=[imf_weo_raw_file, imf_weo_raw_table],
        )
        self.release: tuple = ("April", 2026)

    def _start_mocks(self) -> None:
        self._stack.enter_context(
            patch(
                "dfo_pipelines.defs.imf.weo.assets.weo.get_weo_versions",
                side_effect=lambda: [self.release, ("October", 2025)],
            )
        )
        self._stack.enter_context(
            patch(
                "dfo_pipelines.defs.imf.weo.assets.weo.fetch_data_with_metadata",
                side_effect=lambda version: return_dataframe(FIXTURES / "data.parquet"),
            )
        )
        self._stack.enter_context(
            patch(
                "dfo_pipelines.defs.imf.weo.assets.fetch_weo_publication_date",
                return_value=PUBLICATION_DATE,
            )
        )


def test_imf_weo_source_observation_records_an_observation_event(
    tmp_path: Path,
) -> None:
    """Test observing the source records one observation against the source"""

    with _WEOAutomation(tmp_path) as automation:
        result = automation.observe()

    assert [
        entry.asset_key
        for entry in result.all_events
        if entry.event_type_value == "ASSET_OBSERVATION"
    ] == [imf_weo_source.key]


def test_imf_weo_source_is_requested_once_per_daily_cron_tick(tmp_path: Path) -> None:
    """Test the source's condition requests one observation per UTC day

    The 13:00 tick is what pins the cadence: a shorter cron would request an
    observation an hour after the last one. The 00:01 tick proves the condition
    requests an observation at all once the daily tick has passed, and 00:02
    proves it is not requested again before the next one.
    """
    with _WEOAutomation(tmp_path) as automation:
        # A baseline tick: every condition is reset on its initial evaluation,
        # so the first tick requests nothing whatever the time is.
        automation.source_tick(at=datetime(2026, 8, 25, 12, 0, tzinfo=UTC))

        assert automation.source_tick(at=datetime(2026, 8, 25, 13, 0, tzinfo=UTC)) == 0
        assert automation.source_tick(at=datetime(2026, 8, 26, 0, 1, tzinfo=UTC)) == 1
        assert automation.source_tick(at=datetime(2026, 8, 26, 0, 2, tzinfo=UTC)) == 0


def test_imf_weo_raw_metadata_loads_the_recorded_metadata(tmp_path: Path) -> None:
    """Test the metadata asset loads the extraction metadata with the raw file"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    write_extraction_metadata(
        UPath(tmp_path),
        downloaded_at=DOWNLOADED_AT,
        release_dates={"source": PUBLICATION_DATE},
    )

    result = imf_weo_raw_metadata(
        duckdb=duckdb, raw_file=str(tmp_path / "data.parquet")
    )

    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata == {
        "dagster/table_name": dg.TextMetadataValue("imf_weo.raw_weo_metadata"),
        "dagster/row_count": dg.IntMetadataValue(1),
    }
    table = "imf_weo.raw_weo_metadata"
    assert column_list(duckdb, table) == ["downloaded_at", "source_release_date"]
    assert list_first_row(duckdb, table) == [
        DOWNLOADED_AT,
        pd.Timestamp(PUBLICATION_DATE),
    ]


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
