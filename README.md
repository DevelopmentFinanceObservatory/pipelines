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

Automation runs through one sensor, `automation_condition_sensor`, which is on by default. To pause automation locally, stop the sensor under Automation in the Dagster UI. Its state is kept in `DAGSTER_HOME`, so it stays stopped across `dg dev` restarts until you start it again.

On a fresh environment, materialize the dbt seeds, and any other asset with no parents, once by hand. `eager()` never builds an asset with no parents, so nothing downstream of them automates until they exist.

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

### Pre-commit hooks

To run the checks on each commit, install the hooks:

```bash
uv run pre-commit install
```

To run them on every file:

```bash
uv run pre-commit run --all-files
```

## License

The code in this repository is licensed under the MIT License ([LICENSE](LICENSE)). The data the pipelines download and publish stays under each publisher's terms, recorded in every dataset's provenance.
