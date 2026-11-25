import json
from collections.abc import Callable
from pathlib import Path

import dagster as dg
import pandas as pd
import pytest

from dfo_pipelines.data_commons.export import DOCUMENTATION_FILES, ExportSpec
from dfo_pipelines.defs.one.conversion_tables.assets import (
    DAC_EXPORT_SPEC,
    IMF_EXPORT_SPEC,
    one_conversion_tables_dac_dc_export,
    one_conversion_tables_imf_dc_export,
)
from dfo_pipelines.resources.file_store import UPathFileStoreResource
from tests.helpers import exported_directory, warehouse_with_models

DOCS_DIR = Path(__file__).parents[4] / "docs" / "one" / "conversion_tables"

type ExportAsset = Callable[..., dg.MaterializeResult]

TABLES = {
    "dac": (DAC_EXPORT_SPEC, one_conversion_tables_dac_dc_export),
    "imf": (IMF_EXPORT_SPEC, one_conversion_tables_imf_dc_export),
}


def _provenance_sql(table: str) -> str:
    recipe = table.upper()
    return f"""
create or replace table
    one_conversion_tables.dc_conversion_tables_{table}_provenance
as
select
    'ONE Conversion Tables ({recipe})' as name,
    'source/ONEData' as source,
    'https://example.org/conversion' as url,
    'https://example.org/dac1,https://example.org/weo' as source_data_url,
    'https://example.org/terms' as license,
    'P6M' as source_release_frequency,
    '2026-09-22' as imported_source_release_date,
    '2026-10-01T14:03:11Z' as last_data_refresh_date,
    2024 as deflator_base_year,
    '2023' as earliest_observation_date,
    '2026' as latest_observation_date,
    'Test description' as description,
    'Test attribution' as license_attribution
"""


def _statvars_sql(table: str) -> str:
    recipe = f"ConversionRecipe{table.upper()}"
    return f"""
create or replace table
    one_conversion_tables.dc_conversion_tables_{table}_statvars
as
select * from (values
    (
        'dcid:ExchangeRate_CurrencyUnit_{recipe}',
        'Test rate',
        'dcid:CurrencyUnit',
        'dcid:exchangeRate',
        'dcid:measuredValue',
        'dcid:{recipe}'
    ),
    (
        'dcid:Deflator_EconomicProduction_{recipe}',
        'Test deflator',
        'dcid:EconomicProduction',
        'dcid:deflator',
        'dcid:measuredValue',
        'dcid:{recipe}'
    )
) as statvars (
    dcid, name, population_type, measured_property, stat_type, measurement_qualifier
)
"""


def _observations_sql(table: str) -> str:
    recipe = f"ConversionRecipe{table.upper()}"
    return f"""
create or replace table
    one_conversion_tables.dc_conversion_tables_{table}_observations
as
select
    variable,
    date::integer as date,
    observation_about,
    value::double as value,
    unit,
    measurement_method
from (values
    (
        'dcid:ExchangeRate_CurrencyUnit_{recipe}', 2024, 'dcid:CurrencyEUR',
        0.9246, 'dcid:CurrencyPerUSDollar', null
    ),
    (
        'dcid:ExchangeRate_CurrencyUnit_{recipe}', 2026, 'dcid:CurrencyKES',
        130.5, 'dcid:CurrencyPerUSDollar', 'dcid:IMFProjection'
    ),
    (
        'dcid:Deflator_EconomicProduction_{recipe}', 2023, 'dcid:country/KEN',
        97.5, 'dcid:IndexPoint', null
    )
) as observations (
    variable, date, observation_about, value, unit, measurement_method
)
"""


def _run_export(tmp_path: Path, table: str) -> Path:
    """Materialize one table's export asset over hand-written dc models"""
    _, export_asset = TABLES[table]
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "processed"))
    warehouse = warehouse_with_models(
        tmp_path,
        "create schema if not exists one_conversion_tables",
        _provenance_sql(table),
        _statvars_sql(table),
        _observations_sql(table),
    )

    result = export_asset(
        context=dg.build_asset_context(),
        warehouse=warehouse,
        export_file_store=file_store,
    )
    return exported_directory(result)


def _statvar_nodes(out_dir: Path) -> dict[str, str]:
    """Every StatVar node the bundle declares, keyed on its dcid"""
    return {
        node.splitlines()[0].removeprefix("Node: "): node.strip()
        for node in (out_dir / "custom_nodes.mcf").read_text().split("\n\n")
        if node.startswith("Node: ")
    }


def test_the_tables_export_under_their_own_import_names() -> None:
    """Test each table is its own import, so neither ingest replaces the other"""
    assert DAC_EXPORT_SPEC.import_name == "one_conversion_tables_dac"
    assert IMF_EXPORT_SPEC.import_name == "one_conversion_tables_imf"


@pytest.mark.parametrize("table", TABLES)
def test_dc_export_writes_the_deflator_base_year_as_an_integer(
    tmp_path: Path, table: str
) -> None:
    """Test the Provenance node carries deflatorBaseYear unquoted, as a number"""
    out_dir = _run_export(tmp_path, table)

    provenance = (out_dir / "provenance.mcf").read_text()
    assert "deflatorBaseYear: 2024\n" in provenance
    assert 'lastDataRefreshDate: "2026-10-01T14:03:11Z"\n' in provenance
    assert "source: dcid:source/ONEData\n" in provenance
    assert "conversionRecipe" not in provenance


@pytest.mark.parametrize("table", TABLES)
def test_dc_export_writes_the_pipeline_documentation_onto_the_provenance_node(
    tmp_path: Path, table: str
) -> None:
    """Test both Provenance nodes carry the conversion tables' documentation"""
    out_dir = _run_export(tmp_path, table)

    provenance = (out_dir / "provenance.mcf").read_text()
    for property_name, file_name in DOCUMENTATION_FILES.items():
        first_line = (DOCS_DIR / file_name).read_text().splitlines()[0]
        assert f'{property_name}: "{first_line}' in provenance


@pytest.mark.parametrize("table", TABLES)
def test_dc_export_writes_the_import_config(tmp_path: Path, table: str) -> None:
    """Test the config registers observationAbout, the measurement method and the period"""
    out_dir = _run_export(tmp_path, table)

    provenance = f"dcid:provenance/ONEConversionTables{table.upper()}"
    assert json.loads((out_dir / "config.json").read_text()) == {
        "importName": f"one_conversion_tables_{table}",
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
                    "dcid:observationAbout": "observation_about",
                    "dcid:measurementMethod": "measurement_method",
                },
                "observationProperties": {"observationPeriod": "P1Y"},
                "format": "variablePerRow",
            },
        ],
    }


@pytest.mark.parametrize("table", TABLES)
def test_dc_export_declares_statvars_with_no_observation_properties(
    tmp_path: Path, table: str
) -> None:
    """Test each StatVar reads its subject from observationAbout

    The rate loader in dfo-apis reads a table's rows by observationAbout, which
    is the subject only while the StatVar names no observationProperties.
    """
    out_dir = _run_export(tmp_path, table)

    recipe = f"ConversionRecipe{table.upper()}"
    nodes = _statvar_nodes(out_dir)
    rate = nodes[f"dcid:ExchangeRate_CurrencyUnit_{recipe}"]
    assert "populationType: dcid:CurrencyUnit" in rate
    assert "measuredProperty: dcid:exchangeRate" in rate
    assert f"measurementQualifier: dcid:{recipe}" in rate
    assert 'name: "Test rate"' in rate
    deflator = nodes[f"dcid:Deflator_EconomicProduction_{recipe}"]
    assert "populationType: dcid:EconomicProduction" in deflator
    assert "measuredProperty: dcid:deflator" in deflator
    for node in nodes.values():
        assert "observationProperties" not in node


@pytest.mark.parametrize("table", TABLES)
def test_dc_export_marks_only_projected_rows_with_a_measurement_method(
    tmp_path: Path, table: str
) -> None:
    """Test a reported row's measurement method is blank and a projected row's is IMFProjection"""
    out_dir = _run_export(tmp_path, table)

    spec: ExportSpec = TABLES[table][0]
    mapping = spec.observation_inputs[0].property_to_column
    written = pd.concat(
        pd.read_csv(csv_file) for csv_file in sorted(out_dir.glob("observations_*.csv"))
    ).set_index(mapping["observationAbout"])

    assert written.loc["dcid:CurrencyKES", mapping["measurementMethod"]] == (
        "dcid:IMFProjection"
    )
    assert pd.isna(written.loc["dcid:CurrencyEUR", mapping["measurementMethod"]])
    assert written.loc["dcid:CurrencyEUR", mapping["value"]] == 0.9246
    assert written.loc["dcid:country/KEN", mapping["unit"]] == "dcid:IndexPoint"
