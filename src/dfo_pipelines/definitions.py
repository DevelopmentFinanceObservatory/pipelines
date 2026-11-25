from pathlib import Path

import dagster as dg

from dfo_pipelines.orchestration.environment import Environment, dfo_environment
from dfo_pipelines.resources.climate_finance_export import (
    ClimateFinanceExportResource,
)
from dfo_pipelines.resources.file_store import UPathFileStoreResource
from dfo_pipelines.resources.knowledge_graph import KnowledgeGraphResource
from dfo_pipelines.resources.skills_repository import (
    SkillsRepositoryResource,
)
from dfo_pipelines.resources.warehouse import (
    BigQueryWarehouseResource,
    DuckDBWarehouseResource,
)


@dg.definitions
def defs():
    environment = dfo_environment()
    loaded = dg.load_from_defs_folder(path_within_project=Path(__file__).parent)
    return dg.Definitions.merge(
        loaded,
        dg.Definitions(
            # One step at a time, since DuckDB allows one read-write process.
            executor=dg.multiprocess_executor.configured({"max_concurrent": 1}),
            resources={
                "landing_file_store": UPathFileStoreResource(
                    base_path="data/landing"
                    if environment is Environment.LOCAL
                    else dg.EnvVar("LANDING_STORE_URI")
                ),
                "export_file_store": UPathFileStoreResource(base_path="data/exports"),
                "warehouse": DuckDBWarehouseResource(database=dg.EnvVar("DUCKDB_PATH"))
                if environment is Environment.LOCAL
                else BigQueryWarehouseResource(
                    project=dg.EnvVar("BIGQUERY_PROJECT_ID"),
                    location=dg.EnvVar("BIGQUERY_LOCATION"),
                ),
                "knowledge_graph": KnowledgeGraphResource(
                    project_id=dg.EnvVar("DCP_PROJECT_ID"),
                    region=dg.EnvVar("DCP_REGION"),
                    bucket_name=dg.EnvVar("DCP_BUCKET_NAME"),
                    workflow_name=dg.EnvVar("DCP_WORKFLOW_NAME"),
                ),
                "climate_finance_export": ClimateFinanceExportResource(
                    uri=dg.EnvVar("CLIMATE_FINANCE_EXPORT_URI")
                ),
                "skills_repository": SkillsRepositoryResource(
                    url=dg.EnvVar("DFO_SKILLS_REPOSITORY_URL"),
                ),
            },
        ),
    )
