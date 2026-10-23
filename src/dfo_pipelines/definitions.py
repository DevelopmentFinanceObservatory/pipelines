from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource

from dfo_pipelines.defs.resources import (
    KnowledgeGraphResource,
    SkillsRepositoryResource,
    UPathFileStoreResource,
)


@dg.definitions
def defs():
    loaded = dg.load_from_defs_folder(path_within_project=Path(__file__).parent)
    return dg.Definitions.merge(
        loaded,
        dg.Definitions(
            # One step at a time, since DuckDB allows one read-write process.
            executor=dg.multiprocess_executor.configured({"max_concurrent": 1}),
            resources={
                "raw_file_store": UPathFileStoreResource(base_path="data/raw"),
                "export_file_store": UPathFileStoreResource(base_path="data/processed"),
                "duckdb": DuckDBResource(database=dg.EnvVar("DUCKDB_PATH")),
                "knowledge_graph": KnowledgeGraphResource(
                    project_id=dg.EnvVar("DCP_PROJECT_ID"),
                    region=dg.EnvVar("DCP_REGION"),
                    bucket_name=dg.EnvVar("DCP_BUCKET_NAME"),
                    workflow_name=dg.EnvVar("DCP_WORKFLOW_NAME"),
                ),
                "skills_repository": SkillsRepositoryResource(
                    url=dg.EnvVar("DFO_SKILLS_REPOSITORY_URL"),
                ),
            },
        ),
    )
