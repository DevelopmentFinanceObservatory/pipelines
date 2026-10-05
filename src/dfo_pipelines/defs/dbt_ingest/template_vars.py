import dagster as dg

from dfo_pipelines.defs.automation import EAGER_WHEN_CHECKS_PASS


@dg.template_var
def eager_when_checks_pass() -> dg.AutomationCondition:
    """The dbt models' condition, which component YAML has no `&` or `|` to build"""
    return EAGER_WHEN_CHECKS_PASS
