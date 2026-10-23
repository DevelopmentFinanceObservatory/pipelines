import re
from datetime import date
from pathlib import Path
from typing import override

import dagster as dg
from dcp_tools.gcp_utilities.settings import KGSettings
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource
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


class _ResourceKGSettings(KGSettings):
    """KGSettings built from the values passed in and nothing else

    KGSettings otherwise lets the environment and a .env file override those
    values, and forbids any variable it does not declare, which is every other
    variable in this project.
    """

    @classmethod
    @override
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (init_settings,)


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
    workflow_name: str
    service_account: str | None = None

    @property
    def settings(self) -> KGSettings:
        """Settings as dcp-tools expects them"""
        return _ResourceKGSettings(
            gcp_project_id=self.project_id,
            gcp_region=self.region,
            gcs_bucket_name=self.bucket_name,
            ingestion_workflow_name=self.workflow_name,
            ingestion_service_account=self.service_account,
            # Required by KGSettings but read by nothing on the ingestion path,
            # which is handed the bundle's directory and never writes an output.
            local_path=Path("."),
            gcs_output_folder_path="",
        )


# A GitHub repository's page, which is also the address git fetches it from.
SKILLS_REPOSITORY_URL_SHAPE = re.compile(
    r"https://github\.com/[A-Za-z0-9-]+/[A-Za-z0-9._-]+(?<!\.git)"
)


class SkillsRepositoryResource(dg.ConfigurableResource):
    """GitHub repository the DFO agent skills are fetched from and linked to

    Each deployment has its own, so the URL is read on Dagster's config surface,
    where a missing variable fails a run at launch and names the variable.
    """

    url: str
    branch: str = "main"

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        """Refuse a URL that git cannot fetch or a skill's link cannot extend

        Checked here because a field validator also runs when definitions load,
        where an environment-backed field still holds the variable's name.
        """
        if not SKILLS_REPOSITORY_URL_SHAPE.fullmatch(self.url):
            raise ValueError(
                f"skills repository URL {self.url!r} must have the shape "
                "https://github.com/<owner>/<repository>, with no trailing '/' or "
                "'.git'. Git fetches from it and each skill's link is built by "
                "appending to it"
            )

    def skill_url(self, *, root: str, skill_name: str) -> str:
        """Link to a skill's directory on the branch, under the root it sits in"""
        return f"{self.url}/tree/{self.branch}/{root}/{skill_name}"
