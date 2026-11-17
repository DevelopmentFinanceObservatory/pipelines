"""The sensor that evaluates every asset's automation condition"""

import os

import dagster as dg


def automation_sensor_default_status() -> dg.DefaultSensorStatus:
    """The sensor's status on a fresh instance: `AUTOMATION_SENSOR_DEFAULT_STATUS`, else RUNNING"""
    return dg.DefaultSensorStatus(
        os.getenv("AUTOMATION_SENSOR_DEFAULT_STATUS", "RUNNING")
    )


automation_condition_sensor = dg.AutomationConditionSensorDefinition(
    "automation_condition_sensor",
    target=dg.AssetSelection.all(include_sources=True),
    default_status=automation_sensor_default_status(),
)
