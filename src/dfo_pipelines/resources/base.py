import dagster as dg
from pydantic import ConfigDict


class StrictConfigurableResource(dg.ConfigurableResource):
    """Resource that raises on an undeclared keyword argument instead of ignoring it"""

    # Dagster copies a resource from its __dict__, so a cached_property read
    # before the copy fails as an undeclared field
    model_config = ConfigDict(**dg.ConfigurableResource.model_config, extra="forbid")
