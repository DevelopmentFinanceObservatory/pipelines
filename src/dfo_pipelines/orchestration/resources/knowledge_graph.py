from dcp_tools.gcp_utilities.settings import KGSettings

from dfo_pipelines.orchestration.resources.base import StrictConfigurableResource


class KnowledgeGraphResource(StrictConfigurableResource):
    """Data Commons knowledge graph a bundle is ingested into

    Reading the settings here puts them on Dagster's config surface, where a
    missing one fails a run at launch and names the variable.
    """

    project_id: str
    region: str
    bucket_name: str
    workflow_name: str
    service_account: str | None = None

    @property
    def settings(self) -> KGSettings:
        """Settings as dcp-tools expects them"""
        return KGSettings(
            gcp_project_id=self.project_id,
            gcp_region=self.region,
            gcs_bucket_name=self.bucket_name,
            ingestion_workflow_name=self.workflow_name,
            ingestion_service_account=self.service_account,
            # Passed so the environment and .env cannot set them, until
            # https://github.com/ONEcampaign/dcp-tools/issues/202
            local_path=None,
            gcp_credentials=None,
            gcs_input_folder_path=KGSettings.model_fields[
                "gcs_input_folder_path"
            ].default,
            gcs_output_folder_path=None,
        )
