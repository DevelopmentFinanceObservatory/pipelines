"""The environment the pipelines run in"""

import os
from enum import StrEnum


class Environment(StrEnum):
    LOCAL = "local"
    DEV = "dev"


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
