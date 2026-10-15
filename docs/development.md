# Maintainer notes

Detail for the ONE Data team that runs these pipelines. Contributors do not need it.

## Automation

Each source is observed on a schedule, and the eager assets downstream of it rerun when it publishes new data. The shared conditions are in `src/dfo_pipelines/defs/automation.py`, which also explains why an asset waits while a blocking check on one of its parents has failed.

Automation runs through one sensor, `automation_condition_sensor`, which is on by default. To pause automation locally, stop the sensor under Automation in the Dagster UI. Its state is kept in `DAGSTER_HOME`, so it stays stopped across `dg dev` restarts until you start it again.

On a fresh environment, materialise the dbt seeds, and any other asset with no parents, once by hand. `eager()` never builds an asset with no parents, so nothing downstream of them automates until they exist.

The `*_dc_ingestion` assets have no automation condition. An ingestion rewrites the shared graph and keeps running after the workflow starts, so each is materialised by hand.

## Ingestion settings

Only the ingestion assets read the `DCP_*` variables in `.env.example`: `DCP_PROJECT_ID`, `DCP_REGION`, `DCP_BUCKET_NAME`, `DCP_PREP_JOB_NAME` and `DCP_WORKFLOW_NAME`. They point at a Data Commons Platform instance in Google Cloud (ONE's is the `dev-fin-observatory` project) and are read by `KnowledgeGraphResource` in `src/dfo_pipelines/defs/resources.py`. A run that includes an ingestion asset fails at launch and names the missing variable.

`DFO_SKILLS_REPOSITORY_URL` is the GitHub repository the skills assets fetch from, in the form `https://github.com/<owner>/<repository>` with no trailing slash or `.git`. Each deployment sets its own.

## Dependencies

`dfo-ontology` is installed from the ontology repository at a pinned commit (`[tool.uv.sources]` in `pyproject.toml`), because no package is published to PyPI. Bump `rev` and run `uv lock` to take a newer ontology.

## Dataset pages and changelogs

Each dataset under `docs/<source>/<dataset>` has two pages, matching the Dagster asset group, the dbt schema and the file store prefix of its pipeline:

- `transformations.md` explains what the pipeline does to the source data and always describes the current state of the dataset. Rewrite it in place.
- `changelog.md` holds dated entries for every production release that changes how the data is processed. It is append-only, newest first.

## CI

The workflow in `.github/workflows/ci.yml` runs on every pull request that is not a draft, and again when a draft is marked ready for review. The `lint` job runs ruff, ty and sqlfluff. The `test` job checks that the Dagster definitions load, loads fixtures from `scripts/load_ci_fixtures.sql` into DuckDB, builds the dbt models, and runs pytest with coverage. `lint` and `test` are required checks, and a pull request must be up to date with its base before it merges.
