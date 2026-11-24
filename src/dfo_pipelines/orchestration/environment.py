"""The environment the pipelines run in, and where each keeps its files"""

import os
from enum import StrEnum


class Environment(StrEnum):
    LOCAL = "local"
    DEV = "dev"


LANDING_BASE_PATHS = {
    Environment.LOCAL: "data/landing",
    Environment.DEV: "gs://dev-fin-observatory-pipelines-landing",
}

# TODO: Point dev at gs://dev-fin-observatory-pipelines-exports once the exports
# can write there: https://github.com/DevelopmentFinanceObservatory/pipelines/issues/165
EXPORT_BASE_PATHS = {
    Environment.LOCAL: "data/exports",
    Environment.DEV: "data/exports",
}


def dfo_environment() -> Environment:
    """The environment named by `DFO_ENVIRONMENT`

    Raises:
        ValueError: If it is unset or not an `Environment`.
    """
    value = os.environ.get("DFO_ENVIRONMENT")
    try:
        return Environment(value)
    except ValueError:
        allowed = ", ".join(Environment)
        raise ValueError(
            f"DFO_ENVIRONMENT is {value!r}, and must be one of: {allowed}"
        ) from None
