# DFO data pipelines

Dagster and dbt pipelines that download development-finance data from its publishers, standardise countries, codes and units, and export the result as import bundles for [Data Commons](https://datacommons.org/). They are part of the [Development Finance Observatory](https://github.com/DevelopmentFinanceObservatory), an initiative of [ONE Data](https://data.one.org) ([developmentfinance.org](https://developmentfinance.org)). The organisation profile maps the other repositories.

This repository is under active development. Interfaces, schemas and data may change without notice.

## Sources

Each dataset has a page describing what the pipeline does to the source data, and a changelog.

- OECD DAC1, aggregate flows by provider: [docs/oecd/dac1](docs/oecd/dac1/transformations.md)
- OECD DAC2A, official development assistance disbursements by provider and recipient: [docs/oecd/dac2a](docs/oecd/dac2a/transformations.md)
- OECD DAC2B, other official flows and export credits by provider and recipient: [docs/oecd/dac2b](docs/oecd/dac2b/transformations.md)
- OECD Creditor Reporting System (CRS), activity-level aid: [docs/oecd/dac_crs](docs/oecd/dac_crs/transformations.md)
- World Bank International Debt Statistics (IDS): [docs/world_bank/ids](docs/world_bank/ids/transformations.md)
- IMF World Economic Outlook (WEO): [docs/imf/weo](docs/imf/weo/transformations.md)
- ONE conversion tables, exchange rates and deflators: [docs/one/conversion_tables](docs/one/conversion_tables/transformations.md)
- ONE imputed multilateral spending by sector and recipient: [docs/one/imputed_multilateral_sectors](docs/one/imputed_multilateral_sectors/transformations.md)

The pipelines also load the DFO ontology, from the [ontology repository](https://github.com/DevelopmentFinanceObservatory/ontology), and the DFO agent skills, from the [skills repository](https://github.com/DevelopmentFinanceObservatory/skills).

## What's inside

- `src/dfo_pipelines/`: Dagster assets, resources, and the shared export and ingestion code
- `dbt/`: dbt models, from staging (`stg_`) through intermediate (`int_`) to the Data Commons schema (`dc_`)
- `docs/`: how each source is transformed, one directory per dataset, and the [maintainer notes](docs/development.md)
- `tests/`: Python tests
- `data/`: local raw downloads (`raw/`), exported bundles (`processed/`) and, by default, the DuckDB database

## Getting started

A pipeline runs from download to exported bundle on a laptop with no access to ONE's cloud. Downloads, dbt models and exports write to `data/` and a local DuckDB file. Only the `*_dc_ingestion` assets, which load a bundle into a Data Commons instance, need ONE's Google Cloud project (`dev-fin-observatory`), so an outside reader cannot run them.

Install [uv](https://docs.astral.sh/uv/), then the dependencies and the dbt packages:

```bash
uv sync
uv run dbt deps --project-dir dbt --profiles-dir dbt
```

Copy the example environment file, set `DAGSTER_HOME` and `DUCKDB_PATH` to absolute paths in your checkout, and leave the `DCP_*` variables commented out:

```bash
cp .env.example .env
mkdir .dagster_home
```

Run one pipeline, here World Bank IDS, from download to bundle. The first command downloads the source and loads the raw tables, including the ontology code mapping the dbt models read. The dbt commands load the seeds and build the models. The last command writes the bundle.

```bash
uv run --env-file .env dg launch --assets '*world_bank_ids_raw_table or *world_bank_ids_raw_metadata or *dfo_ontology_raw_code_mapping_table'
uv run --env-file .env dbt seed --project-dir dbt --profiles-dir dbt
uv run --env-file .env dbt run --project-dir dbt --profiles-dir dbt --select '+path:models/world_bank/ids'
uv run --env-file .env dg launch --assets world_bank_ids_dc_export
```

The bundle is written under `data/processed/world_bank_ids/`, in a directory partitioned by date. To work in the Dagster UI instead, run `uv run --env-file .env dg dev` and open http://localhost:3000.

To build and test every dbt model, once every dataset's raw tables exist in DuckDB:

```bash
uv run --env-file .env dbt build --project-dir dbt --profiles-dir dbt
```

## Documentation

- [Dataset pages](docs/README.md): what each pipeline does to its source
- [Maintainer notes](docs/development.md): automation, ingestion settings, CI and releases

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Licence

MIT. See [LICENSE](LICENSE). The data the pipelines download stays under each publisher's terms, recorded in every dataset's provenance.
