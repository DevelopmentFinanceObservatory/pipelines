import logging
import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

import dagster as dg
from dcp_tools.gcp_utilities.settings import KGSettings
from pydantic import ConfigDict
from upath import UPath


class StrictConfigurableResource(dg.ConfigurableResource):
    """Resource that raises on an undeclared keyword argument instead of ignoring it"""

    # Dagster copies a resource from its __dict__, so a cached_property read
    # before the copy fails as an undeclared field
    model_config = ConfigDict(**dg.ConfigurableResource.model_config, extra="forbid")


class UPathFileStoreResource(StrictConfigurableResource):
    """File store for UPath backends"""

    base_path: str

    @contextmanager
    def write_once_directory(
        self, path: str, *, created_at: datetime, run_id: str, log: logging.Logger
    ) -> Iterator[UPath]:
        """Create `<base_path>/<path>/<created_at in UTC>_<run_id>` for the block to
        write, and remove it if the block raises

        The run ID keeps runs that start in the same second apart.
        """
        directory = (
            UPath(self.base_path)
            / path
            / f"{created_at.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')}_{run_id}"
        )
        directory.mkdir(parents=True)
        try:
            yield directory
        except BaseException:
            # Raise the failure that stopped the write, not one from cleaning up.
            try:
                directory.rmdir(recursive=True)
            except Exception as error:
                log.warning(f"Could not remove {directory}: {error}")
            raise


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


# A GitHub repository's page, which is also the address git fetches it from.
SKILLS_REPOSITORY_URL_SHAPE = re.compile(
    r"https://github\.com/[A-Za-z0-9-]+/[A-Za-z0-9._-]+(?<!\.git)"
)


class SkillsRepositoryResource(StrictConfigurableResource):
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


class ClimateFinanceExportResource(StrictConfigurableResource):
    """Where the climate finance pipeline reads its release's raw tables

    `uri` is an export root that `scripts/export_climate_finance_release.py`
    writes, a `gs://` URI or a local directory.
    """

    uri: str
