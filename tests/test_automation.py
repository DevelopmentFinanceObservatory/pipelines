from collections.abc import Set
from functools import cache

import dagster as dg
import pytest

from dfo_pipelines.definitions import defs
from dfo_pipelines.defs.automation import automation_sensor_default_status

SENSOR_NAME = "automation_condition_sensor"

# The shared mappings every pipeline resolves codes against.
CODE_MAPPING_KEY = dg.AssetKey(["dfo_ontology", "code_mapping"])
STAT_TYPE_MAPPING_KEY = dg.AssetKey(["data_commons_mappings", "stat_type_mapping"])

SHARED_MAPPING_KEYS = {
    CODE_MAPPING_KEY,
    STAT_TYPE_MAPPING_KEY,
}

ONTOLOGY_RAW_TABLE_KEY = dg.AssetKey("dfo_ontology_raw_code_mapping_table")

# What code_mapping waits on: the compiled export, now loaded as a table, and
# the two seeds holding the mappings the ontology has not adopted yet and the
# codes it leaves to the pipeline.
CODE_MAPPING_PARENT_KEYS = {
    ONTOLOGY_RAW_TABLE_KEY,
    dg.AssetKey(["dfo_ontology", "interim_code_mapping"]),
    dg.AssetKey(["dfo_ontology", "ids_code_mapping"]),
}

IDS_MEASURE_MAPPING_KEY = dg.AssetKey(["world_bank_ids", "ids_measure_mapping"])
IDS_UNIT_MAPPING_KEY = dg.AssetKey(["world_bank_ids", "ids_unit_mapping"])

IDS_SOURCE_KEY = dg.AssetKey("world_bank_ids_source")
IDS_RAW_FILE_KEY = dg.AssetKey("world_bank_ids_raw_file")
IDS_RAW_TABLE_KEY = dg.AssetKey("world_bank_ids_raw_table")
IDS_RAW_METADATA_KEY = dg.AssetKey("world_bank_ids_raw_metadata")
IDS_EXPORT_KEY = dg.AssetKey("world_bank_ids_dc_export")

IDS_DBT_MODEL_NAMES = (
    "stg_ids",
    "int_ids_pruned",
    "int_ids_resolved",
    "int_ids_unified",
    "int_ids_mapped",
    "int_ids_observations",
    "dc_ids_statvars",
    "dc_ids_debt_observations",
    "dc_ids_economy_observations",
    "dc_ids_provenance",
)

IDS_DBT_MODEL_KEYS = {
    dg.AssetKey(["world_bank_ids", name]) for name in IDS_DBT_MODEL_NAMES
}

IDS_ASSET_KEYS = {
    IDS_SOURCE_KEY,
    IDS_RAW_FILE_KEY,
    IDS_RAW_TABLE_KEY,
    IDS_RAW_METADATA_KEY,
    IDS_EXPORT_KEY,
} | IDS_DBT_MODEL_KEYS

# What the IDS models wait on from outside their own pipeline: int_ids_resolved refs
# code_mapping, and int_ids_mapped refs that plus the three mapping seeds the DC
# schema layer resolves its measures and units against.
IDS_PARENT_KEYS = {
    CODE_MAPPING_KEY,
    STAT_TYPE_MAPPING_KEY,
    IDS_MEASURE_MAPPING_KEY,
    IDS_UNIT_MAPPING_KEY,
    IDS_RAW_METADATA_KEY,
}

CRS_SOURCE_KEY = dg.AssetKey("oecd_dac_crs_source")
CRS_RAW_FILE_KEY = dg.AssetKey("oecd_dac_crs_raw_file")
CRS_RAW_TABLE_KEY = dg.AssetKey("oecd_dac_crs_raw_table")
CRS_RAW_METADATA_KEY = dg.AssetKey("oecd_dac_crs_raw_metadata")
CRS_EXPORT_KEY = dg.AssetKey("oecd_dac_crs_dc_export")

DAC1_SOURCE_KEY = dg.AssetKey("oecd_dac1_source")
DAC1_RAW_FILE_KEY = dg.AssetKey("oecd_dac1_raw_file")
DAC1_RAW_TABLE_KEY = dg.AssetKey("oecd_dac1_raw_table")
DAC1_RAW_METADATA_KEY = dg.AssetKey("oecd_dac1_raw_metadata")
DAC1_EXPORT_KEY = dg.AssetKey("oecd_dac1_dc_export")

DAC2A_SOURCE_KEY = dg.AssetKey("oecd_dac2a_source")
DAC2A_RAW_FILE_KEY = dg.AssetKey("oecd_dac2a_raw_file")
DAC2A_RAW_TABLE_KEY = dg.AssetKey("oecd_dac2a_raw_table")
DAC2A_RAW_METADATA_KEY = dg.AssetKey("oecd_dac2a_raw_metadata")
DAC2A_EXPORT_KEY = dg.AssetKey("oecd_dac2a_dc_export")

DAC2B_SOURCE_KEY = dg.AssetKey("oecd_dac2b_source")
DAC2B_RAW_FILE_KEY = dg.AssetKey("oecd_dac2b_raw_file")
DAC2B_RAW_TABLE_KEY = dg.AssetKey("oecd_dac2b_raw_table")
DAC2B_RAW_METADATA_KEY = dg.AssetKey("oecd_dac2b_raw_metadata")
DAC2B_EXPORT_KEY = dg.AssetKey("oecd_dac2b_dc_export")

# The seven dbt models under models/oecd/dac_crs, keyed by dbt schema + name
CRS_DBT_MODEL_NAMES = (
    "stg_dac_crs",
    "int_dac_crs_mapped",
    "int_dac_crs_resolved",
    "int_dac_crs_unified",
    "dc_dac_crs_observations",
    "dc_dac_crs_provenance",
    "dc_dac_crs_statvars",
)
CRS_DBT_MODEL_KEYS = {
    dg.AssetKey(["oecd_dac_crs", name]) for name in CRS_DBT_MODEL_NAMES
}

CRS_ASSET_KEYS = {
    CRS_SOURCE_KEY,
    CRS_RAW_FILE_KEY,
    CRS_RAW_TABLE_KEY,
    CRS_RAW_METADATA_KEY,
    CRS_EXPORT_KEY,
} | CRS_DBT_MODEL_KEYS

DAC1_DBT_MODEL_NAMES = (
    "stg_dac1",
    "int_dac1_resolved",
    "int_dac1_unified",
    "int_dac1_mapped",
    "dc_dac1_donor_observations",
    "dc_dac1_flow_observations",
    "dc_dac1_provenance",
    "dc_dac1_statvars",
)
DAC1_DBT_MODEL_KEYS = {
    dg.AssetKey(["oecd_dac1", name]) for name in DAC1_DBT_MODEL_NAMES
}

DAC1_ASSET_KEYS = {
    DAC1_SOURCE_KEY,
    DAC1_RAW_FILE_KEY,
    DAC1_RAW_TABLE_KEY,
    DAC1_RAW_METADATA_KEY,
    DAC1_EXPORT_KEY,
} | DAC1_DBT_MODEL_KEYS

# What the DAC1 models wait on from outside the pipeline.
DAC1_PARENT_KEYS = SHARED_MAPPING_KEYS | {
    DAC1_RAW_METADATA_KEY,
    dg.AssetKey(["oecd_dac1", "dac1_indicator_mapping"]),
}

DAC2A_DBT_MODEL_NAMES = (
    "stg_dac2a",
    "int_dac2a_resolved",
    "int_dac2a_unified",
    "int_dac2a_mapped",
    "dc_dac2a_observations",
    "dc_dac2a_provenance",
    "dc_dac2a_statvars",
)
DAC2A_DBT_MODEL_KEYS = {
    dg.AssetKey(["oecd_dac2a", name]) for name in DAC2A_DBT_MODEL_NAMES
}

DAC2A_ASSET_KEYS = {
    DAC2A_SOURCE_KEY,
    DAC2A_RAW_FILE_KEY,
    DAC2A_RAW_TABLE_KEY,
    DAC2A_RAW_METADATA_KEY,
    DAC2A_EXPORT_KEY,
} | DAC2A_DBT_MODEL_KEYS

# What the DAC2a models wait on from outside the pipeline.
DAC2A_PARENT_KEYS = SHARED_MAPPING_KEYS | {
    DAC2A_RAW_METADATA_KEY,
    dg.AssetKey(["oecd_dac2a", "dac2a_indicator_mapping"]),
}

DAC2B_DBT_MODEL_NAMES = (
    "stg_dac2b",
    "int_dac2b_resolved",
    "int_dac2b_unified",
    "int_dac2b_mapped",
    "dc_dac2b_observations",
    "dc_dac2b_provenance",
    "dc_dac2b_statvars",
)
DAC2B_DBT_MODEL_KEYS = {
    dg.AssetKey(["oecd_dac2b", name]) for name in DAC2B_DBT_MODEL_NAMES
}

DAC2B_ASSET_KEYS = {
    DAC2B_SOURCE_KEY,
    DAC2B_RAW_FILE_KEY,
    DAC2B_RAW_TABLE_KEY,
    DAC2B_RAW_METADATA_KEY,
    DAC2B_EXPORT_KEY,
} | DAC2B_DBT_MODEL_KEYS

# What the DAC2b models wait on from outside the pipeline.
DAC2B_PARENT_KEYS = SHARED_MAPPING_KEYS | {
    DAC2B_RAW_METADATA_KEY,
    dg.AssetKey(["oecd_dac2b", "dac2b_indicator_mapping"]),
}

DFO_SKILLS_ASSET_KEYS = {
    dg.AssetKey("dfo_skills_source"),
    dg.AssetKey("dfo_skills_raw_directory"),
    dg.AssetKey("dfo_skills_dc_export"),
}

DFO_SHARED_LICENSE_TYPES_KEY = dg.AssetKey(["dfo_shared", "license_types"])
DFO_SHARED_EXPORT_KEY = dg.AssetKey("dfo_shared_dc_export")

DFO_SHARED_ASSET_KEYS = {DFO_SHARED_LICENSE_TYPES_KEY, DFO_SHARED_EXPORT_KEY}

CRS_PARENT_KEYS = SHARED_MAPPING_KEYS | {CRS_RAW_METADATA_KEY}

IMS_SOURCE_KEY = dg.AssetKey("one_imputed_multilateral_sectors_source")
IMS_RAW_FILE_KEY = dg.AssetKey("one_imputed_multilateral_sectors_raw_file")
IMS_RAW_TABLE_KEY = dg.AssetKey("one_imputed_multilateral_sectors_raw_table")
IMS_RAW_METADATA_KEY = dg.AssetKey("one_imputed_multilateral_sectors_raw_metadata")
IMS_EXPORT_KEY = dg.AssetKey("one_imputed_multilateral_sectors_dc_export")

# The seven dbt models under models/one/imputed_multilateral_sectors, keyed by dbt
# schema + name
IMS_DBT_MODEL_NAMES = (
    "stg_imputed_multilateral_sectors",
    "int_imputed_multilateral_sectors_resolved",
    "int_imputed_multilateral_sectors_aggregated",
    "int_imputed_multilateral_sectors_mapped",
    "dc_imputed_multilateral_sectors_observations",
    "dc_imputed_multilateral_sectors_provenance",
    "dc_imputed_multilateral_sectors_statvars",
)
IMS_DBT_MODEL_KEYS = {
    dg.AssetKey(["one_imputed_multilateral_sectors", name])
    for name in IMS_DBT_MODEL_NAMES
}

IMS_ASSET_KEYS = {
    IMS_SOURCE_KEY,
    IMS_RAW_FILE_KEY,
    IMS_RAW_TABLE_KEY,
    IMS_RAW_METADATA_KEY,
    IMS_EXPORT_KEY,
} | IMS_DBT_MODEL_KEYS

IMS_PARENT_KEYS = SHARED_MAPPING_KEYS | {IMS_RAW_METADATA_KEY}

WEO_SOURCE_KEY = dg.AssetKey("imf_weo_source")
WEO_RAW_FILE_KEY = dg.AssetKey("imf_weo_raw_file")
WEO_RAW_TABLE_KEY = dg.AssetKey("imf_weo_raw_table")
WEO_RAW_METADATA_KEY = dg.AssetKey("imf_weo_raw_metadata")
WEO_EXPORT_KEY = dg.AssetKey("imf_weo_dc_export")

WEO_DBT_MODEL_NAMES = (
    "stg_weo",
    "int_weo_resolved",
    "int_weo_unified",
    "int_weo_dollar_countries",
    "int_weo_dollar_twins",
    "int_weo_mapped",
    "dc_weo_statvars",
    "dc_weo_observations",
    "dc_weo_provenance",
)
WEO_DBT_MODEL_KEYS = {dg.AssetKey(["imf_weo", name]) for name in WEO_DBT_MODEL_NAMES}

WEO_ASSET_KEYS = {
    WEO_SOURCE_KEY,
    WEO_RAW_FILE_KEY,
    WEO_RAW_TABLE_KEY,
    WEO_RAW_METADATA_KEY,
    WEO_EXPORT_KEY,
} | WEO_DBT_MODEL_KEYS

WEO_PARENT_KEYS = {
    CODE_MAPPING_KEY,
    STAT_TYPE_MAPPING_KEY,
    dg.AssetKey(["imf_weo", "weo_indicator_mapping"]),
    dg.AssetKey(["imf_weo", "weo_currency_mapping"]),
    dg.AssetKey(["imf_weo", "weo_extra_statvars"]),
    dg.AssetKey(["imf_weo", "weo_source"]),
    dg.AssetKey(["imf_weo", "weo_staged_indicators"]),
    WEO_RAW_METADATA_KEY,
}

CONVERSION_TABLES_DAC_EXPORT_KEY = dg.AssetKey("one_conversion_tables_dac_dc_export")
CONVERSION_TABLES_IMF_EXPORT_KEY = dg.AssetKey("one_conversion_tables_imf_dc_export")

# The dbt models under models/one/conversion_tables, keyed by dbt schema + name
CONVERSION_TABLES_DBT_MODEL_NAMES = (
    "int_conversion_tables_dac1_resolved",
    "int_conversion_tables_dac1_series",
    "int_conversion_tables_weo_series",
    "int_conversion_tables_dac_rate_members",
    "int_conversion_tables_dac_rate_references",
    "int_conversion_tables_dac_rates",
    "int_conversion_tables_dac_rate_agreement",
    "int_conversion_tables_dac_rate_band",
    "int_conversion_tables_dac_deflators_unrebased",
    "int_conversion_tables_dac_deflators",
    "int_conversion_tables_imf_rate_members",
    "int_conversion_tables_imf_rate_references",
    "int_conversion_tables_imf_rates",
    "int_conversion_tables_imf_rate_agreement",
    "int_conversion_tables_imf_deflators_unrebased",
    "int_conversion_tables_imf_deflators",
    "int_conversion_tables_statvars",
    "int_conversion_tables_dac_observations",
    "int_conversion_tables_imf_observations",
    "dc_conversion_tables_dac_observations",
    "dc_conversion_tables_dac_provenance",
    "dc_conversion_tables_dac_statvars",
    "dc_conversion_tables_imf_observations",
    "dc_conversion_tables_imf_provenance",
    "dc_conversion_tables_imf_statvars",
)
CONVERSION_TABLES_DBT_MODEL_KEYS = {
    dg.AssetKey(["one_conversion_tables", name])
    for name in CONVERSION_TABLES_DBT_MODEL_NAMES
}

CONVERSION_TABLES_ASSET_KEYS = {
    CONVERSION_TABLES_DAC_EXPORT_KEY,
    CONVERSION_TABLES_IMF_EXPORT_KEY,
} | CONVERSION_TABLES_DBT_MODEL_KEYS

# What the conversion tables wait on from outside their own models.
CONVERSION_TABLES_PARENT_KEYS = SHARED_MAPPING_KEYS | {
    dg.AssetKey(["oecd_dac1", "stg_dac1"]),
    dg.AssetKey(["oecd_dac1_raw_metadata"]),
    dg.AssetKey(["imf_weo", "int_weo_unified"]),
    dg.AssetKey(["imf_weo_raw_metadata"]),
    dg.AssetKey(["imf_weo", "weo_currency_mapping"]),
    dg.AssetKey(["one_conversion_tables", "currency_rate_reference"]),
}


@cache
def _project_definitions() -> dg.Definitions:
    """The project's definitions, loaded as dg dev and dg check defs load them"""
    return defs()


def _project_sensors() -> list[dg.SensorDefinition]:
    """Every sensor the project's definitions declare"""
    return list(_project_definitions().sensors or [])


def _automation_sensor() -> dg.AutomationConditionSensorDefinition:
    """The declared automation sensor"""
    sensor = next(sensor for sensor in _project_sensors() if sensor.name == SENSOR_NAME)
    assert isinstance(sensor, dg.AutomationConditionSensorDefinition)
    return sensor


def _report_runs(
    instance: dg.DagsterInstance,
    materialized_keys: set[dg.AssetKey],
    failed_check_keys: frozenset[dg.AssetCheckKey] = frozenset(),
) -> None:
    """Report runless materializations of `materialized_keys` with their checks

    Their checks are reported as passing, as a real run evaluates them, except
    `failed_check_keys`.
    """
    graph = _project_definitions().resolve_asset_graph()
    for asset_key in materialized_keys:
        instance.report_runless_asset_event(
            dg.AssetMaterialization(asset_key=asset_key)
        )
    for check_key in graph.get_check_keys_for_assets(materialized_keys):
        instance.report_runless_asset_event(
            dg.AssetCheckEvaluation(
                asset_key=check_key.asset_key,
                check_name=check_key.name,
                passed=check_key not in failed_check_keys,
            )
        )


def _requested_counts(
    requested_keys: set[dg.AssetKey],
    materialized_keys: set[dg.AssetKey],
    failed_check_keys: frozenset[dg.AssetCheckKey] = frozenset(),
) -> dict[str, int]:
    """Count what one automation tick requests after `materialized_keys` materialize"""
    project = _project_definitions()
    selection = dg.AssetSelection.assets(*requested_keys)

    with dg.DagsterInstance.ephemeral() as instance:
        # A baseline tick, which resets every condition on its initial
        # evaluation and so requests nothing whatever has happened.
        baseline = dg.evaluate_automation_conditions(
            defs=project, instance=instance, asset_selection=selection
        )

        _report_runs(instance, materialized_keys, failed_check_keys)

        requested = dg.evaluate_automation_conditions(
            defs=project,
            instance=instance,
            asset_selection=selection,
            cursor=baseline.cursor,
        )

    return {
        key.to_user_string(): requested.get_num_requested(key) for key in requested_keys
    }


def _first_requested_ticks(
    asset_keys: set[dg.AssetKey], materialized_keys: set[dg.AssetKey], *, max_ticks: int
) -> dict[dg.AssetKey, int]:
    """Run successive automation ticks over `asset_keys` until the cascade settles

    Reports a runless materialization, with passing checks, for each of
    `materialized_keys` after a baseline tick, then repeatedly evaluates the
    conditions of `asset_keys`, materializes whatever was requested, and
    evaluates again with the returned cursor - the same tick-then-materialize
    loop a running sensor performs one poll at a time. Stops the first tick
    that requests nothing. Fails the test outright if `max_ticks` is reached
    first, so a condition that never settles does not hang the suite.

    Returns:
        dict[dg.AssetKey, int]: Every asset that was requested at least once,
        mapped to the 1-indexed tick it was first requested on.
    """
    project = _project_definitions()
    selection = dg.AssetSelection.assets(*asset_keys)

    first_requested_tick: dict[dg.AssetKey, int] = {}

    with dg.DagsterInstance.ephemeral() as instance:
        # A baseline tick, which resets every condition on its initial
        # evaluation and so requests nothing whatever has happened.
        baseline = dg.evaluate_automation_conditions(
            defs=project, instance=instance, asset_selection=selection
        )

        _report_runs(instance, materialized_keys)

        cursor = baseline.cursor

        for tick in range(1, max_ticks + 1):
            result = dg.evaluate_automation_conditions(
                defs=project,
                instance=instance,
                asset_selection=selection,
                cursor=cursor,
            )

            requested_this_tick = [
                key for key in asset_keys if result.get_num_requested(key) > 0
            ]

            if not requested_this_tick:
                break

            for key in requested_this_tick:
                first_requested_tick.setdefault(key, tick)
            _report_runs(instance, set(requested_this_tick))

            cursor = result.cursor
        else:
            pytest.fail(
                f"automation cascade over {selection} did not settle within "
                f"{max_ticks} ticks"
            )

    return first_requested_tick


def test_automation_sensor_is_the_only_sensor() -> None:
    """Test the declared sensor is loaded, and that Dagster adds none of its own

    Dagster creates a `default_automation_condition_sensor` for any asset that
    no declared sensor targets, and creates it stopped. A second sensor here
    means part of the graph is left to a sensor nobody has switched on.
    """
    assert [sensor.name for sensor in _project_sensors()] == [SENSOR_NAME]


def test_automation_sensor_runs_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test the sensor starts without a manual UI toggle when nothing sets its status

    Sensor on/off state lives in a gitignored, per-environment schedules.db, so
    a sensor that is not default-running has to be found and switched on in
    every environment it is deployed to - so, check it defaults to RUNNING.
    """
    monkeypatch.delenv("AUTOMATION_SENSOR_DEFAULT_STATUS", raising=False)

    assert automation_sensor_default_status() == dg.DefaultSensorStatus.RUNNING


@pytest.mark.parametrize(
    "asset_keys",
    [
        CRS_ASSET_KEYS,
        IDS_ASSET_KEYS,
        IMS_ASSET_KEYS,
        WEO_ASSET_KEYS,
        DAC1_ASSET_KEYS,
        DAC2A_ASSET_KEYS,
        DAC2B_ASSET_KEYS,
        DFO_SKILLS_ASSET_KEYS,
        DFO_SHARED_ASSET_KEYS,
        CONVERSION_TABLES_ASSET_KEYS,
    ],
    ids=[
        "crs",
        "ids",
        "ims",
        "weo",
        "dac1",
        "dac2a",
        "dac2b",
        "dfo_skills",
        "dfo_shared",
        "conversion_tables",
    ],
)
def test_automation_sensor_targets_the_automated_assets(
    asset_keys: set[dg.AssetKey],
) -> None:
    """Test the sensor evaluates every asset of an automated pipeline

    The observable source is the critical one: if the sensor does not target it,
    its daily cron never fires and nothing downstream is ever gated. The
    selection is AssetSelection.all(include_sources=True), so the project's own
    definitions are loaded to resolve it against the real asset graph - which is
    also the only place the dbt asset keys exist.
    """
    sensor = _automation_sensor()

    selected = sensor.asset_selection.resolve(
        _project_definitions().resolve_asset_graph()
    )

    assert asset_keys <= selected


@pytest.mark.parametrize(
    ("dbt_model_keys", "raw_table_key", "parent_keys"),
    [
        (
            CRS_DBT_MODEL_KEYS,
            CRS_RAW_TABLE_KEY,
            CRS_PARENT_KEYS,
        ),
        (IDS_DBT_MODEL_KEYS, IDS_RAW_TABLE_KEY, IDS_PARENT_KEYS),
        (IMS_DBT_MODEL_KEYS, IMS_RAW_TABLE_KEY, IMS_PARENT_KEYS),
        (WEO_DBT_MODEL_KEYS, WEO_RAW_TABLE_KEY, WEO_PARENT_KEYS),
        (DAC1_DBT_MODEL_KEYS, DAC1_RAW_TABLE_KEY, DAC1_PARENT_KEYS),
        (DAC2A_DBT_MODEL_KEYS, DAC2A_RAW_TABLE_KEY, DAC2A_PARENT_KEYS),
        (DAC2B_DBT_MODEL_KEYS, DAC2B_RAW_TABLE_KEY, DAC2B_PARENT_KEYS),
    ],
    ids=["crs", "ids", "ims", "weo", "dac1", "dac2a", "dac2b"],
)
def test_dbt_models_are_requested_when_the_raw_table_updates(
    dbt_model_keys: set[dg.AssetKey],
    raw_table_key: dg.AssetKey,
    parent_keys: set[dg.AssetKey],
) -> None:
    """Test the dbt models for each pipeline are automated on upstream table materialization

    Their automation conditions come from the `translation` block in
    dbt_ingest/defs.yaml, so the project's own definitions are loaded to
    exercise that YAML. The out-of-pipeline parents get runless materializations
    too, as eager() waits for every parent to have been materialized at least
    once.
    """
    requested_counts = _requested_counts(dbt_model_keys, {raw_table_key, *parent_keys})

    expected_counts = {key.to_user_string(): 1 for key in dbt_model_keys}

    assert requested_counts == expected_counts


def test_conversion_tables_are_requested_when_their_parents_update() -> None:
    """Test the conversion-table models and both exports automate on upstream updates

    The tables have no raw asset of their own: they are computed from the DAC1
    raw table and the WEO unified layer, so every parent outside the pipeline
    gets a runless materialization and every model and export must be requested.
    """
    requested_counts = _requested_counts(
        CONVERSION_TABLES_ASSET_KEYS, CONVERSION_TABLES_PARENT_KEYS
    )

    assert requested_counts == {
        key.to_user_string(): 1 for key in CONVERSION_TABLES_ASSET_KEYS
    }


def test_ims_source_cascades_through_dbt_models_to_the_export() -> None:
    """Test a source update reaches the export with no manual trigger anywhere in between

    This is issue #144's completion criterion: materializing
    `one_imputed_multilateral_sectors_source` should, with nobody stepping in to
    trigger an intermediate asset by hand, eventually get
    `one_imputed_multilateral_sectors_dc_export` requested too. The two shared
    mapping models, `code_mapping` and `stat_type_mapping`, are materialized
    here as a precondition rather than as part of the cascade under test: they
    come from the ontology pipeline, whose root asset
    `dfo_ontology_raw_directory` carries no automation condition, so a
    deployment materializes it once by hand before any pipeline's dbt models
    can run.

    `_first_requested_ticks` walks the cascade tick by tick rather than
    reporting every materialization up front, so it can record, for every
    asset in the pipeline, the tick it was first requested on. That lets this
    test assert not just that the export is eventually requested, but that it
    is never requested ahead of the raw table - i.e. as a downstream effect of
    the raw table's update, not independently of it. In practice both land on
    the same tick: Dagster's `eager()` treats a dependency that `will_be_requested`
    this tick as satisfying `~any_deps_missing()` for its dependents, so a brand
    new pipeline's whole eligible frontier, from the raw table down to the
    export, resolves in one evaluation rather than trickling out one level per
    poll.
    """
    first_requested_tick = _first_requested_ticks(
        IMS_ASSET_KEYS, SHARED_MAPPING_KEYS | {IMS_SOURCE_KEY}, max_ticks=15
    )

    for key in (
        *IMS_DBT_MODEL_KEYS,
        IMS_RAW_FILE_KEY,
        IMS_RAW_TABLE_KEY,
        IMS_RAW_METADATA_KEY,
    ):
        assert key in first_requested_tick, (
            f"{key.to_user_string()} was never requested by the cascade"
        )

    # Asserted by name, not just as a set member: the export reaching
    # automation is the fact this test exists to prove.
    assert IMS_EXPORT_KEY in first_requested_tick, (
        "one_imputed_multilateral_sectors_dc_export was never requested by the cascade"
    )
    assert (
        first_requested_tick[IMS_EXPORT_KEY] >= first_requested_tick[IMS_RAW_TABLE_KEY]
    )


@pytest.mark.parametrize(
    ("withheld_parent_key", "still_requested_model_names"),
    [
        pytest.param(
            CODE_MAPPING_KEY,
            ("stg_dac_crs",),
            id="code_mapping",
        ),
        pytest.param(
            STAT_TYPE_MAPPING_KEY,
            ("stg_dac_crs", "int_dac_crs_resolved", "int_dac_crs_unified"),
            id="stat_type_mapping",
        ),
    ],
)
def test_an_unmaterialized_crs_seed_stops_the_cascade_at_its_dependent(
    withheld_parent_key: dg.AssetKey,
    still_requested_model_names: tuple[str, ...],
) -> None:
    """Test one unmaterialized parent blocks the model that refs it, and all below it

    eager() ends in ~any_deps_missing(), and none of these parents declares an
    automation condition of its own, so one is never "will be requested" and one
    that has never been materialized blocks its dependent for good.

    The `test_dbt_models_are_requested_when_the_raw_table_updates` test above
    only evaluates the fully seeded graph where every parent has a
    materialization and ~any_deps_missing() is therefore vacuously true. This
    test checks that when a parent is withheld, the model that refs it is not
    requested, and neither is anything that refs it.
    """
    still_requested_model_keys = {
        dg.AssetKey(["oecd_dac_crs", name]) for name in still_requested_model_names
    }

    requested_counts = _requested_counts(
        CRS_DBT_MODEL_KEYS,
        {CRS_RAW_TABLE_KEY, *(CRS_PARENT_KEYS - {withheld_parent_key})},
    )

    expected_counts = {
        key.to_user_string(): 1 if key in still_requested_model_keys else 0
        for key in CRS_DBT_MODEL_KEYS
    }

    assert requested_counts == expected_counts


@pytest.mark.parametrize(
    ("withheld_parent_key", "still_requested_model_names"),
    [
        pytest.param(
            CODE_MAPPING_KEY,
            ("stg_ids", "int_ids_pruned"),
            id="code_mapping",
        ),
        pytest.param(
            STAT_TYPE_MAPPING_KEY,
            ("stg_ids", "int_ids_pruned", "int_ids_resolved", "int_ids_unified"),
            id="stat_type_mapping",
        ),
        pytest.param(
            IDS_MEASURE_MAPPING_KEY,
            ("stg_ids", "int_ids_pruned", "int_ids_resolved", "int_ids_unified"),
            id="ids_measure_mapping",
        ),
        pytest.param(
            IDS_UNIT_MAPPING_KEY,
            ("stg_ids", "int_ids_pruned", "int_ids_resolved", "int_ids_unified"),
            id="ids_unit_mapping",
        ),
    ],
)
def test_an_unmaterialized_mapping_stops_the_ids_cascade_at_its_dependent(
    withheld_parent_key: dg.AssetKey,
    still_requested_model_names: tuple[str, ...],
) -> None:
    """Test one unmaterialized mapping blocks the model that refs it, and all below it

    The IDS counterpart to the CRS test above. Each of these parents declares no
    automation condition of its own, so on a new environment one that has never
    been materialized blocks its dependent for good.
    """
    still_requested_model_keys = {
        dg.AssetKey(["world_bank_ids", name]) for name in still_requested_model_names
    }

    requested_counts = _requested_counts(
        IDS_DBT_MODEL_KEYS,
        {IDS_RAW_TABLE_KEY, *(IDS_PARENT_KEYS - {withheld_parent_key})},
    )

    expected_counts = {
        key.to_user_string(): 1 if key in still_requested_model_keys else 0
        for key in IDS_DBT_MODEL_KEYS
    }

    assert requested_counts == expected_counts


def test_code_mapping_is_requested_once_its_parents_are_materialized() -> None:
    """Test a refreshed export or re-seeded mapping rebuilds the model"""
    requested_counts = _requested_counts({CODE_MAPPING_KEY}, CODE_MAPPING_PARENT_KEYS)

    assert requested_counts == {CODE_MAPPING_KEY.to_user_string(): 1}


def test_shared_export_is_requested_when_the_license_types_seed_updates() -> None:
    """Test a re-seeded licence types table rebuilds the shared export"""
    requested_counts = _requested_counts(
        {DFO_SHARED_EXPORT_KEY}, {DFO_SHARED_LICENSE_TYPES_KEY}
    )

    assert requested_counts == {DFO_SHARED_EXPORT_KEY.to_user_string(): 1}


def test_dbt_translation_predicate_matches_intended_schemas() -> None:
    """Test the translation predicate automates only the intended dbt schemas"""
    graph = _project_definitions().resolve_asset_graph()

    automated_dbt_schemas = {
        key.path[0]
        for key in graph.get_all_asset_keys()
        if "dbt" in graph.get(key).kinds and graph.get(key).automation_condition
    }

    assert automated_dbt_schemas == {
        "world_bank_ids",
        "oecd_dac1",
        "oecd_dac2a",
        "oecd_dac2b",
        "oecd_dac_crs",
        "dfo_ontology",
        "one_imputed_multilateral_sectors",
        "one_conversion_tables",
        "imf_weo",
    }


@pytest.mark.parametrize(
    ("parent", "check_name", "child"),
    [
        ("oecd_dac_crs_raw_file", "file_not_empty", "oecd_dac_crs_raw_table"),
        ("oecd_dac_crs_raw_table", "table_not_empty", "oecd_dac_crs/stg_dac_crs"),
        (
            "oecd_dac_crs_raw_table",
            "crs_required_columns_present",
            "oecd_dac_crs/stg_dac_crs",
        ),
        (
            "oecd_dac_crs/stg_dac_crs",
            "not_null_stg_dac_crs_donor_code",
            "oecd_dac_crs/int_dac_crs_resolved",
        ),
        (
            "oecd_dac_crs/dc_dac_crs_observations",
            "not_null_dc_dac_crs_observations_date",
            "oecd_dac_crs_dc_export",
        ),
    ],
    ids=[
        "python_to_python",
        "python_to_dbt",
        "dbt_source_test_to_dbt",
        "dbt_to_dbt",
        "dbt_to_python",
    ],
)
def test_a_failed_blocking_check_stops_the_child_being_requested(
    parent: str, check_name: str, child: str
) -> None:
    """Test a failed blocking check on a parent stops the child on the next tick

    Python assets and dbt models are given the condition in different places, so
    each kind of edge is covered.
    """
    child_key = dg.AssetKey.from_user_string(child)
    parent_keys = (
        _project_definitions().resolve_asset_graph().get(child_key).parent_keys
    )
    failed = dg.AssetCheckKey(dg.AssetKey.from_user_string(parent), check_name)

    assert _requested_counts({child_key}, set(parent_keys)) == {child: 1}
    assert _requested_counts({child_key}, set(parent_keys), frozenset({failed})) == {
        child: 0
    }


def test_a_failed_check_recovers_on_new_upstream_data() -> None:
    """Test new upstream data requests a failed parent and its child on one tick"""
    project = _project_definitions()
    graph = project.resolve_asset_graph()
    upstream = dg.AssetKey("oecd_dac_crs_raw_table")
    parent = dg.AssetKey(["oecd_dac_crs", "stg_dac_crs"])
    child = dg.AssetKey(["oecd_dac_crs", "int_dac_crs_resolved"])
    failed = dg.AssetCheckKey(parent, "not_null_stg_dac_crs_donor_code")

    with dg.DagsterInstance.ephemeral() as instance:

        def tick(
            previous: dg.EvaluateAutomationConditionsResult | None,
        ) -> tuple[dg.EvaluateAutomationConditionsResult, list[int]]:
            result = dg.evaluate_automation_conditions(
                defs=project,
                instance=instance,
                asset_selection=dg.AssetSelection.assets(parent, child),
                cursor=previous.cursor if previous else None,
            )
            return result, [result.get_num_requested(key) for key in (parent, child)]

        baseline, _ = tick(None)
        _report_runs(
            instance, {upstream, *graph.get(child).parent_keys}, frozenset({failed})
        )
        after_failure, blocked = tick(baseline)
        _report_runs(instance, {upstream})
        _, recovered = tick(after_failure)

    assert blocked == [0, 0]
    assert recovered == [1, 1]


# The DAC1 chain, which the IMF table must never read.
DAC1_LINEAGE_KEYS = {DAC1_RAW_TABLE_KEY, dg.AssetKey(["oecd_dac1", "stg_dac1"])} | {
    dg.AssetKey(["one_conversion_tables", name])
    for name in (
        "int_conversion_tables_dac1_resolved",
        "int_conversion_tables_dac1_series",
    )
}


def test_the_imf_conversion_table_reads_no_dac1_asset() -> None:
    """Test no DAC1 asset is upstream of the IMF export

    A failing check on any upstream asset withholds the export, so a DAC1 asset
    upstream of the IMF export would let a DAC1 problem freeze conversions of
    IMF and World Bank data. The DAC export reads the DAC1 chain, which shows the
    keys name assets that exist.
    """
    graph = _project_definitions().resolve_asset_graph()

    def upstream(key: dg.AssetKey) -> Set[dg.AssetKey]:
        return dg.AssetSelection.assets(key).upstream().resolve(graph)

    assert DAC1_LINEAGE_KEYS <= upstream(CONVERSION_TABLES_DAC_EXPORT_KEY)
    assert not DAC1_LINEAGE_KEYS & upstream(CONVERSION_TABLES_IMF_EXPORT_KEY)


@pytest.mark.parametrize(
    ("diagnostic_model", "check_name", "stopped_export_keys"),
    [
        (
            "int_conversion_tables_dac_rate_agreement",
            "every_member_agrees_with_its_reference",
            {CONVERSION_TABLES_DAC_EXPORT_KEY},
        ),
        (
            "int_conversion_tables_dac_rate_band",
            "the_two_sources_share_currency_years",
            {CONVERSION_TABLES_DAC_EXPORT_KEY},
        ),
        (
            "int_conversion_tables_dac_rate_band",
            "dac1_and_weo_rates_agree_within_a_factor_of_two",
            {CONVERSION_TABLES_DAC_EXPORT_KEY},
        ),
        (
            "int_conversion_tables_imf_rate_agreement",
            "every_member_agrees_with_its_reference",
            {CONVERSION_TABLES_DAC_EXPORT_KEY, CONVERSION_TABLES_IMF_EXPORT_KEY},
        ),
    ],
    ids=["dac_agreement", "dac_band_overlap", "dac_band_ratio", "imf_agreement"],
)
def test_a_failed_rate_diagnostic_stops_every_export_reading_its_table(
    diagnostic_model: str, check_name: str, stopped_export_keys: set[dg.AssetKey]
) -> None:
    """Test a failed rate diagnostic withholds every export that reads its table

    The rate diagnostics feed no publication model, so they gate the exports only
    as export dependencies. A DAC-only diagnostic leaves the IMF export
    requested. The DAC table fills from the IMF rates, so the IMF agreement
    check withholds both exports.
    """
    export_keys = {CONVERSION_TABLES_DAC_EXPORT_KEY, CONVERSION_TABLES_IMF_EXPORT_KEY}
    materialized_keys = CONVERSION_TABLES_PARENT_KEYS | CONVERSION_TABLES_DBT_MODEL_KEYS
    failed = dg.AssetCheckKey(
        dg.AssetKey(["one_conversion_tables", diagnostic_model]), check_name
    )

    assert _requested_counts(export_keys, materialized_keys) == {
        key.to_user_string(): 1 for key in export_keys
    }
    assert _requested_counts(export_keys, materialized_keys, frozenset({failed})) == {
        key.to_user_string(): 0 if key in stopped_export_keys else 1
        for key in export_keys
    }
