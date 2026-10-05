"""Dagster automation definitions."""

import dagster as dg

automation_condition_sensor = dg.AutomationConditionSensorDefinition(
    "automation_condition_sensor",
    target=dg.AssetSelection.all(include_sources=True),
    default_status=dg.DefaultSensorStatus.RUNNING,
)

# A blocking check only stops the rest of its own run, so eager() alone would
# still request the dependents of an asset whose check failed. A dependency
# requested on the same tick runs in the same run, where its failing check stops
# this asset; otherwise its blocking checks must have passed.
EAGER_WHEN_CHECKS_PASS = (
    dg.AutomationCondition.eager()
    & dg.AutomationCondition.all_deps_match(
        dg.AutomationCondition.will_be_requested()
        | dg.AutomationCondition.all_checks_match(
            dg.AutomationCondition.check_passed(), blocking_only=True
        )
    )
).with_label("eager_when_checks_pass")
