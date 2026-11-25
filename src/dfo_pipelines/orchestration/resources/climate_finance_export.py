from dfo_pipelines.orchestration.resources.base import StrictConfigurableResource


class ClimateFinanceExportResource(StrictConfigurableResource):
    """Where the climate finance pipeline reads its release's raw tables

    `uri` is an export root that `scripts/export_climate_finance_release.py`
    writes, a `gs://` URI or a local directory.
    """

    uri: str
