# dfo-pipelines

Data pipelines for the Development Finance Observatory.

## Installation

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
# Install dependencies
uv sync
```

One dependency, `dfo-ontology`, is resolved from a private repository, so `git` has to be
able to authenticate to GitHub before `uv sync` will succeed. This step goes away when that
repository is made public.

## Configuration

Environment variables are loaded from a local `.env` file:

- `DAGSTER_HOME`: directory where Dagster stores run history and metadata, so they persist across `dg dev` restarts.
- `DUCKDB_PATH`: path to the DuckDB database that the pipelines read and write to.
- `DCP_PROJECT_ID`, `DCP_REGION`, `DCP_BUCKET_NAME`, `DCP_PREP_JOB_NAME` and `DCP_WORKFLOW_NAME`: the Data Commons Platform instance that bundles are ingested into. Only the ingestion assets read them, so the rest of the project runs without them.

Copy the example file and replace `path/to/repo` with the project root on your machine.

```bash
# Create the .env file
cp .env.example .env
```

## Running Dagster

This project uses [Dagster](https://docs.dagster.io/) for pipeline orchestration.

```bash
# Start the Dagster UI web server
uv run dg dev
```

Open http://localhost:3000 in your browser to see the project.

## Automation

This project defines an `automation_condition_sensor` in code and starts it by default, so no manual UI toggle is required. The sensor evaluates every asset, but only the assets that declare an automation condition are ever requested automatically — everything else runs when it is materialized by hand.

- `world_bank_ids_source` is observed daily at 00:00 UTC with HTTP `HEAD` requests.
- `world_bank_ids_raw_file`, `world_bank_ids_raw_table` and the IDS dbt models downstream of the raw table rerun when the observed source data version changes, or when one of their upstream assets is materialized by hand. Like the CRS models, they wait for their seeds to be materialized once: without `interim_code_mapping` and `ids_code_mapping` the chain stops at `stg_ids`, and without `stat_type_mapping`, `ids_measure_mapping` or `ids_unit_mapping` at `int_ids_unified`.
- `world_bank_ids_dc_export` materializes the Data Commons import bundle — `config.json`, the MCF nodes and the observation CSVs under `data/processed/world_bank_ids/` — when the three `dc_ids_*` models it reads are materialized.
- `oecd_dac_crs_source` is observed daily at 00:00 UTC via the OECD SDMX API.
- `oecd_dac_crs_raw_file`, `oecd_dac_crs_raw_table` and the CRS dbt models downstream of the raw table rerun when the observed source data version changes, or when one of their upstream assets is materialized by hand.
- `oecd_dac_crs_dc_ingestion` declares no automation condition, so the CRS bundle is ingested into the knowledge graph only when it is materialized by hand. The ontology and the shared nodes have to be ingested first, since the bundle links to nodes from both.
- `oecd_dac1_source` is observed daily at 00:00 UTC via the OECD SDMX API.
- `oecd_dac1_raw_file`, `oecd_dac1_raw_table`, `oecd_dac1_raw_metadata` and the DAC1 dbt models downstream of the raw table rerun when the observed source data version changes, or when one of their upstream assets is materialized by hand.
- `oecd_dac1_dc_export` materializes the Data Commons import bundle — `config.json`, the MCF nodes and the observation CSVs under `data/processed/oecd_dac1/` — when the `dc_dac1_*` models it reads are materialized.
- `oecd_dac1_dc_ingestion` declares no automation condition, so the DAC1 bundle is ingested into the knowledge graph only when it is materialized by hand. The ontology, the shared nodes and the IDS bundle have to be ingested first, since the bundle links to nodes from each, including the GNI StatVar the IDS import defines.
- `oecd_dac2a_source` is observed daily at 00:00 UTC via the OECD SDMX API.
- `oecd_dac2a_raw_file`, `oecd_dac2a_raw_table` and the DAC2a dbt models downstream of the raw table rerun when the observed source data version changes, or when one of their upstream assets is materialized by hand.
- `oecd_dac2b_source` is observed daily at 00:00 UTC via the OECD SDMX API.
- `oecd_dac2b_raw_file`, `oecd_dac2b_raw_table` and the DAC2b dbt models downstream of the raw table rerun when the observed source data version changes, or when one of their upstream assets is materialized by hand.
- `one_imputed_multilateral_sectors_source` is observed daily at 00:00 UTC by resolving the CRS and Members' Total Use bulk file versions through `oda_reader`.
- `one_imputed_multilateral_sectors_raw_file`, `one_imputed_multilateral_sectors_raw_table`, `one_imputed_multilateral_sectors_raw_metadata` and the dbt models downstream of the raw table rerun when the observed source data version changes, or when one of their upstream assets is materialized by hand. `int_imputed_multilateral_sectors_resolved` refs `code_mapping`, so like the CRS and IDS models it only begins automating once the mapping seeds have been materialized at least once in that environment.
- The CRS dbt models only begin automating once each of their seed parents — `interim_code_mapping`, `ids_code_mapping` and `stat_type_mapping` — has been materialized at least once in that environment, because `eager()` waits for every parent to have been materialized. Materialize the seeds once by hand on a new environment, or the chain stops at `stg_dac_crs`. Putting the seeds on an automation condition does not fix this: having no parents of their own, `eager()` reduces to `newly_missing()` for them, which needs a transition observed *after* the condition was applied and so never fires for an asset that is already missing.
- `dfo_ontology_raw_directory` declares no automation condition, so the ontology is compiled only when it is materialized by hand. Everything downstream of it automates except `dfo_ontology_dc_ingestion`, so refreshing the ontology is one manual materialization and the rest of the chain follows. Ingesting the refreshed bundle into the knowledge graph is a second manual materialization, of `dfo_ontology_dc_ingestion`. On a new environment the raw directory has to be materialized at least once, or `code_mapping` never builds and both the CRS and IDS chains stop. `dfo_skills_dc_export` takes `dfo_ontology_raw_directory` as an input too, so it waits on that same manual materialization.
- `dfo_skills_source` is observed daily at 00:00 UTC with `git ls-remote` against the dfo-skills `main` branch.
- `dfo_skills_raw_directory` and `dfo_skills_dc_export` rerun when the observed commit changes, or when one of their upstream assets is materialized by hand.
- Each skill's `attachments.yaml` in dfo-skills lists the ontology nodes the skill applies to and the export provenances whose series it explains. `dfo_skills_dc_export` writes them as `skillAppliesTo` and `skillAppliesToProvenance` on the skill node, and the file itself stays out of the bundle.
- `dfo_skills_dc_ingestion` declares no automation condition, so the skills bundle is ingested into the knowledge graph only when it is materialized by hand. The ontology has to be ingested first, since the skills link to its nodes.
- `dfo_shared_dc_export` reruns when the `license_types` seed is materialized by hand. `dfo_shared_dc_ingestion` declares no automation condition, so the shared nodes are ingested only when it is materialized by hand, after the ontology. On a new environment, materialize the seed before the CRS chain runs: `dc_dac_crs_provenance` tests its licence type against it.
- `imf_weo_source` is observed daily at 00:00 UTC via the IMF SDMX API.
- `imf_weo_raw_file`, `imf_weo_raw_table`, `imf_weo_raw_metadata` and the WEO dbt models downstream of the raw table rerun when the observed release changes, or when one of their upstream assets is materialized by hand. `stg_weo` keeps only the indicators in the `weo_staged_indicators` seed, so it waits for that seed to be materialized once, and reruns when the seed is materialized again after an indicator is added.
- `imf_weo_dc_export` materializes the Data Commons import bundle — `config.json`, the MCF nodes and the observation CSVs under `data/processed/imf_weo/` — when the `dc_weo_*` models it reads are materialized.
- `imf_weo_dc_ingestion` declares no automation condition, so the WEO bundle is ingested into the knowledge graph only when it is materialized by hand. The ontology and the shared nodes have to be ingested first, since the bundle links to nodes from both.
- The conversion-table dbt models under `models/one/conversion_tables` have no source of their own and rerun when `stg_dac1` or the WEO models they read are materialized, or when the `currency_rate_reference` or `weo_currency_mapping` seed is materialized again. Like the other pipelines they wait for `code_mapping`, `stat_type_mapping` and their seeds to be materialized once. `one_conversion_tables_dac_dc_export` and `one_conversion_tables_imf_dc_export` then write the two bundles under `data/processed/one_conversion_tables_dac/` and `data/processed/one_conversion_tables_imf/`. Each export waits on the models and rate checks of every table it reads. The IMF table reads no DAC1 model, so a DAC1 failure stops only the DAC export, while the DAC table fills from the IMF table's rates, so an IMF failure, the IMF rate-agreement check included, stops both. `one_conversion_tables_dac_dc_ingestion` and `one_conversion_tables_imf_dc_ingestion` declare no automation condition, so each bundle is ingested into the knowledge graph only when it is materialized by hand. The ontology and the shared nodes have to be ingested first, since both bundles link to nodes from each.

An automated asset is not requested while a blocking check on one of its parents has failed, so bad data stops where the check fails rather than reaching the Data Commons export. A parent still materializes when its check fails; the chain resumes on the next good upstream run, or when the parent is materialized again by hand.

Automation only runs while the Dagster daemon is running, which locally means while `uv run dg dev` is open. If the machine is off at 00:00, Dagster catches up on the next daemon evaluation after startup.

## Running dbt

dbt models can be run independently of Dagster if desired, and should be run from the *repo root*.

```bash
# Build the dbt models
uv run --env-file .env dbt build --project-dir dbt --profiles-dir dbt
```

## Project Structure

- `src/dfo_pipelines/`: Dagster pipeline code
- `dbt/`: dbt models
- `data/raw/`: Landing place for raw data when running locally
- `data/processed/`: Landing place for output when running locally

## Code Quality

```bash
# Run linter
uv run ruff check .

# Run formatter
uv run ruff format .

# Run type checker
uv run ty check

# Run SQL linter
uv run --env-file .env sqlfluff lint dbt/models
```

## Pre-commit Hooks

Pre-commit hooks are configured to run automatically. To manually run:

```bash
pre-commit run --all-files
```

## Testing

```bash
# Python tests
uv run pytest

# Dagster definitions load cleanly
uv run dg check defs
```

## License

This project is licensed under the mit License - see the LICENSE file for details.
