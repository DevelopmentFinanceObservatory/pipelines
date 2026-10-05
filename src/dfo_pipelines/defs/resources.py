from datetime import date
from pathlib import Path

import dagster as dg
from dcp_tools.gcp_utilities.settings import KGSettings
from upath import UPath


class UPathFileStoreResource(dg.ConfigurableResource):
    """File store for UPath backends"""

    base_path: str

    def build_date_partitioned_path(
        self, path: str, *, partition_date: date | None = None
    ) -> UPath:
        """Builds a path in the format `<base_path>/<path>/<YYYY>/<MM>/<DD>`.

        The date partition is today's unless partition_date is given.
        """
        partition = (partition_date or date.today()).strftime("%Y/%m/%d")
        return UPath(self.base_path) / path / partition


class KnowledgeGraphResource(dg.ConfigurableResource):
    """Data Commons knowledge graph a bundle is ingested into

    dcp-tools can read its own settings from the environment, but it rejects any
    variable it does not recognise. Reading them here instead puts them on
    Dagster's config surface, where a missing one fails a run at launch and names
    the variable.
    """

    project_id: str
    region: str
    bucket_name: str
    prep_job_name: str
    workflow_name: str
    service_account: str | None = None

    @property
    def settings(self) -> KGSettings:
        """Settings as dcp-tools expects them"""
        return KGSettings(
            gcp_project_id=self.project_id,
            gcp_region=self.region,
            gcs_bucket_name=self.bucket_name,
            ingestion_prep_job_name=self.prep_job_name,
            ingestion_workflow_name=self.workflow_name,
            ingestion_service_account=self.service_account,
            # Required by KGSettings but read by nothing on the ingestion path,
            # which is handed the bundle's directory and never writes an output.
            local_path=Path("."),
            gcs_output_folder_path="",
            # KGSettings otherwise reads .env itself and forbids any variable it
            # does not declare, which is every other variable in this project.
            _env_file=None,
        )
