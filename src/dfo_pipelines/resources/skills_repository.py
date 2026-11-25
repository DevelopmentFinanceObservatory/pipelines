import re

import dagster as dg

from dfo_pipelines.resources.base import StrictConfigurableResource

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
