# dfo-pipelines

Data pipelines for the [Development Finance Observatory](https://developmentfinance.org). Each pipeline downloads a source's data from its publisher, standardises its countries, codes and units, and ingests it into the DFO's knowledge graph, which is built on [Data Commons](https://datacommons.org/).

## Installation

This project uses [uv](https://docs.astral.sh/uv/) for dependency management. Install uv first, and `uv sync` installs the Python version the project needs along with its dependencies. `dbt deps` then installs the dbt packages the models use.

```bash
uv sync
uv run dbt deps --project-dir dbt --profiles-dir dbt
```

## Configuration

Copy the example environment file, [`.env.example`](.env.example), and fill in the values.

```bash
cp .env.example .env
```

## Project structure

- `src/dfo_pipelines/`: Dagster assets, resources and the shared export and ingestion code
- `dbt/`: dbt models, from staging (`stg_`) through intermediate (`int_`) to the Data Commons schema (`dc_`)
- `docs/`: how each source is transformed, one directory per dataset
- `tests/`: Python tests
- `data/`: local raw downloads (`raw/`), exported bundles (`processed/`) and, by default, the DuckDB database (`dfo.duckdb`)

## Running Dagster

This project uses [Dagster](https://docs.dagster.io/) for pipeline orchestration. Start the Dagster UI, then open http://localhost:3000.

```bash
uv run dg dev
```

### Automation

Each source is observed on a schedule, and the eager assets downstream of it rerun when it publishes new data. The shared conditions are in `src/dfo_pipelines/defs/automation.py`, which also explains why an asset waits while a blocking check on one of its parents has failed.

Automation runs through one sensor, `automation_condition_sensor`. On a new local instance it starts stopped, because `.env.example` sets `AUTOMATION_SENSOR_DEFAULT_STATUS=STOPPED`. Materialize what you need by hand, or start the sensor under Automation in the Dagster UI to run every pipeline. Its state is kept in `DAGSTER_HOME`, so it stays as you left it across `dg dev` restarts.

On a fresh environment, materialize the dbt seeds, and any other asset with no parents, once by hand. `eager()` never builds an asset with no parents, so nothing downstream of them automates until they exist.

### Ingestion

The identity that starts the ingestion workflow also needs `roles/workflows.viewer`. Each `*_dc_ingestion` asset reads the workflow's definition before it uploads the bundle, and stops if the workflow comes from a Data Commons Platform stack the installed dcp-tools can't start.

## Running dbt

The dbt models can also be built without Dagster. Run this from the repository root:

```bash
uv run --env-file .env dbt build --project-dir dbt --profiles-dir dbt
```

## Development

### Testing

```bash
# Python tests
uv run pytest

# Dagster definitions load cleanly
uv run dg check defs
```

### Code quality

```bash
# Lint Python
uv run ruff check .

# Format Python
uv run ruff format .

# Type check
uv run ty check

# Lint SQL
uv run --env-file .env sqlfluff lint dbt/models
```

### Graph check comment

Every pull request that is not a draft gets a comment saying whether the StatVar tables its build writes name classes, properties and values the knowledge graph holds, and naming the commit it checked. The CI `test` job posts it after its other steps and updates the same comment on every later push. The job skips draft pull requests, so a draft keeps the comment from its last push before it became a draft. The comment is advisory and never blocks a merge. The check stops after 900 requests or 8 minutes. A check that stops early or does not run says why, and reports what it did check. A pull request from a fork cannot post the comment, and the job summary carries the report or says why the check did not run.

The repository variable `GRAPH_CHECK_URL` holds the address of the graph API's check route. When it is unset, the report says the check did not run.

To run the check on a database a build wrote:

```bash
DUCKDB_PATH=/tmp/ci.duckdb GRAPH_CHECK_URL=<check route URL> uv run python scripts/graph_check.py report.md
```

The script writes the report to the file it is given and prints the report's first line. It exits 0 when the check produced a report, and 1 after writing a report that says the check could not complete.

### Pre-commit hooks

The hooks in `.pre-commit-config.yaml` are the checks CI's lint job runs, on every file. The sqlfluff hook needs `dbt deps` and a `.env` that sets `DUCKDB_PATH`, as above. To run the checks on each commit, install the hooks:

```bash
uv run pre-commit install
```

To run them on every file:

```bash
uv run pre-commit run --all-files
```

## License

The code in this repository is licensed under the MIT License ([LICENSE](LICENSE)). The data the pipelines download and publish stays under each publisher's terms, recorded in every dataset's provenance.
