"""The ONE conversion tables: exchange rates and US-dollar deflators.

The tables are computed in dbt from DAC1 and the WEO, so this module holds only
the two exports and their ingestions. Each table is its own import under its own
provenance, because an ingest replaces everything carrying its import's provenance
and the two tables refresh with different sources.
"""

import dagster as dg

from dfo_pipelines.data_commons.export import (
    DOCS_ROOT,
    ExportSpec,
    ObservationInput,
)
from dfo_pipelines.defs.dfo.ontology.assets import dfo_ontology_dc_ingestion
from dfo_pipelines.defs.dfo.shared.assets import dfo_shared_dc_ingestion
from dfo_pipelines.orchestration.dc_export import build_dc_export_asset
from dfo_pipelines.orchestration.dc_ingestion import build_dc_ingestion_asset

SCHEMA = "one_conversion_tables"
DOCS_DIR = DOCS_ROOT / "one" / "conversion_tables"


def _export_spec(*, table: str) -> ExportSpec:
    """The export of one conversion table, `dac` or `imf`."""
    return ExportSpec(
        import_name=f"one_conversion_tables_{table}",
        provenance_dcid=f"ONEConversionTables{table.upper()}",
        provenance_model=f"{SCHEMA}.dc_conversion_tables_{table}_provenance",
        conversion_recipe=None,
        statvar_model=f"{SCHEMA}.dc_conversion_tables_{table}_statvars",
        observation_inputs=(
            ObservationInput(
                model=f"{SCHEMA}.dc_conversion_tables_{table}_observations",
                property_to_column={
                    "variable": "variable",
                    "date": "date",
                    "value": "value",
                    "unit": "unit",
                    "observationAbout": "observation_about",
                    # Null on a reported value, so only projected rows carry it.
                    "measurementMethod": "measurement_method",
                },
            ),
        ),
        statvar_column_to_property={"name": "name"},
        docs_dir=DOCS_DIR,
        constant_observation_properties={"observationPeriod": "P1Y"},
        provenance_column_to_property={"deflator_base_year": "deflatorBaseYear"},
    )


DAC_EXPORT_SPEC = _export_spec(table="dac")
IMF_EXPORT_SPEC = _export_spec(table="imf")


# The rate diagnostics feed no publication model, so their blocking checks gate
# an export only through its dependencies. Each export waits on the diagnostics
# of every table it reads: the DAC table fills from the IMF table's rates, so the
# DAC export also waits on the IMF agreement check, and the IMF export, which
# reads no DAC1 model, waits on that check alone.
RATE_DIAGNOSTICS = {
    "dac": ("dac_rate_agreement", "dac_rate_band", "imf_rate_agreement"),
    "imf": ("imf_rate_agreement",),
}


def _diagnostic_keys(*, table: str) -> list[dg.AssetKey]:
    return [
        dg.AssetKey([SCHEMA, f"int_conversion_tables_{diagnostic}"])
        for diagnostic in RATE_DIAGNOSTICS[table]
    ]


one_conversion_tables_dac_dc_export = build_dc_export_asset(
    spec=DAC_EXPORT_SPEC,
    group="one_conversion_tables",
    title="the DAC conversion table",
    deps=_diagnostic_keys(table="dac"),
)
one_conversion_tables_imf_dc_export = build_dc_export_asset(
    spec=IMF_EXPORT_SPEC,
    group="one_conversion_tables",
    title="the IMF conversion table",
    deps=_diagnostic_keys(table="imf"),
)


one_conversion_tables_dac_dc_ingestion = build_dc_ingestion_asset(
    import_name=DAC_EXPORT_SPEC.import_name,
    group="one_conversion_tables",
    title="DAC conversion table",
    deps=[dfo_ontology_dc_ingestion, dfo_shared_dc_ingestion],
)
one_conversion_tables_imf_dc_ingestion = build_dc_ingestion_asset(
    import_name=IMF_EXPORT_SPEC.import_name,
    group="one_conversion_tables",
    title="IMF conversion table",
    deps=[dfo_ontology_dc_ingestion, dfo_shared_dc_ingestion],
)
