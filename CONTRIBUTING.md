# Contributing

Issues and pull requests are welcome. The ONE Data team maintains this repository. For a large change, open an issue to discuss it before you write it.

Everyone taking part follows the [Code of Conduct](https://github.com/DevelopmentFinanceObservatory/.github/blob/main/CODE_OF_CONDUCT.md). Report a security problem through GitHub private vulnerability reporting (the "Report a vulnerability" button under the Security tab), and see the [security policy](https://github.com/DevelopmentFinanceObservatory/.github/blob/main/SECURITY.md). [SUPPORT.md](https://github.com/DevelopmentFinanceObservatory/.github/blob/main/SUPPORT.md) says where to ask other questions.

## Where to raise what

- Send a figure that looks wrong, or a question about how a source is modelled, to the feedback form on the [DFO website](https://developmentfinance.org) or to data@one.org.
- Request a new source or dataset with "Submit Request" on the [roadmap](https://developmentfinance.org/data/roadmap).
- Open an issue in this repository for a bug or failing step in a pipeline, a setup problem, or an error in these docs.
- Open an issue in the [ontology repository](https://github.com/DevelopmentFinanceObservatory/ontology) for a problem with a concept, property or code list.
- Open an issue in the [skills repository](https://github.com/DevelopmentFinanceObservatory/skills) to suggest an improvement to an agent skill.

## Setting up

Setup needs no access to ONE's cloud. Follow [Getting started](README.md#getting-started) in the README, which installs the dependencies with [uv](https://docs.astral.sh/uv/) and runs one pipeline locally. The ingestion assets that load a bundle into Data Commons need ONE's Google Cloud project to run, but their tests run without it.

## Before you open a pull request

Run the checks CI runs:

```bash
uv run ruff check .
uv run ruff format --check .
uv run ty check
uv run --env-file .env sqlfluff lint dbt/models --dialect duckdb
uv run dg check defs
uv run pytest
```

To run the formatters and hygiene checks on each commit, install the hooks with `uv run pre-commit install`. To run them on every file, use `uv run pre-commit run --all-files`.

## Pull requests

- Open pull requests against `dev`, the default branch.
- Address one issue and link it. The [pull request template](.github/pull_request_template.md) lists what to fill in.
- Add or update tests for the change, and the dataset's page under `docs/` if the pipeline's behaviour changes.
- Pass the `lint` and `test` CI checks. CI skips draft pull requests, so mark yours ready for review to start it.
- Write commit messages and pull request titles as [Conventional Commits](https://www.conventionalcommits.org/), scoped to the pipeline where one applies.

Maintainer notes on automation, ingestion and CI are in [docs/development.md](docs/development.md).
