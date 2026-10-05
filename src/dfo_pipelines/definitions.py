from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource

from dfo_pipelines.defs.resources import KnowledgeGraphResource, UPathFileStoreResource


@dg.definitions
def defs():
    loaded = dg.load_from_defs_folder(path_within_project=Path(__file__).parent)
    return dg.Definitions.merge(
        loaded,
        dg.Definitions(
            resources={
                "raw_file_store": UPathFileStoreResource(base_path="data/raw"),
                "export_file_store": UPathFileStoreResource(base_path="data/processed"),
                "duckdb": DuckDBResource(database=dg.EnvVar("DUCKDB_PATH")),
                "knowledge_graph": KnowledgeGraphResource(
                    project_id=dg.EnvVar("DCP_PROJECT_ID"),
                    region=dg.EnvVar("DCP_REGION"),
                    bucket_name=dg.EnvVar("DCP_BUCKET_NAME"),
                    prep_job_name=dg.EnvVar("DCP_PREP_JOB_NAME"),
                    workflow_name=dg.EnvVar("DCP_WORKFLOW_NAME"),
                ),
            }
        ),
    )
