"""Automation conditions the pipelines share"""

import dagster as dg

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

# eager() ignores code versions, and an export's code version is its documentation.
EAGER_ON_DEPS_OR_CODE_CHANGE_WHEN_CHECKS_PASS = EAGER_WHEN_CHECKS_PASS.replace(
    old=dg.AutomationCondition.any_deps_updated(),
    new=dg.AutomationCondition.any_deps_updated()
    | dg.AutomationCondition.code_version_changed(),
).with_label("eager_on_deps_or_code_change_when_checks_pass")
