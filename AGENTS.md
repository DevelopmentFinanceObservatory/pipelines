# AGENTS.md

## Making Changes

- Run every step of every job in `.github/workflows/ci.yml` from a script, against a DuckDB file of its own for each worktree, deleted before each run. CI runs a bare `dbt build`, and a narrowed `--select` proves less than it appears to.
- After renaming or moving dbt models, run `dbt parse --warn-error`. dbt drops the tests of a `models.yml` entry and the config of a `dbt_project.yml` path that no longer match a model, with only a warning.
- Prove each new test by breaking the code it covers: make the smallest change that makes its claim false, watch it fail, restore, and watch it pass. A contract can stop the mutated model building, so rebuild everything downstream before running the test.
- Changes to pipelines follow the `building-pipelines` skill in `.claude/skills/`.

## Code Review Rules

This section covers how to review a pull request in this repo.

- Verify all findings before commenting
- Drop low-priority findings that will be made irrelevant by higher-priority findings
- Report blocking findings apart from the rest, each with the concrete failure it causes
- When responding to a review, answer every finding with a fix, a reasoned decline, or why it is a false positive
- For a pull request that touches a pipeline (`src/dfo_pipelines/defs/`, `dbt/`), also check it against `.claude/skills/building-pipelines/references/traps.md`, and that it holds only what its layer owns in `.claude/skills/building-pipelines/references/layers.md`

The following sections are in roughly priority order.

### Scope

- Start by reading the issue that the PR is addressing
- The PR should address a single issue, flag if it combines multiple or has added scope
- Flag changes the issue doesn't need: renames, reflows, moved code, and tests for behaviour the PR doesn't change

### Design and Architecture

- Check for clear separation of concerns between components
- Verify idiomatic use of the relevant frameworks e.g. Dagster, dbt etc.
- Flag brittle functionality

### Code Quality

- Flag anything unused or stale e.g. dependencies, classes, functions, modules, fixtures etc.
- Flag unnecessary code duplication, including in tests
- Verify idiomatic use of the relevant language e.g. Python, SQL etc.
- Verify clear and accurate naming of classes, functions, parameters, variables, tests etc.
- Flag typos
- Flag language inconsistencies e.g. use `materialization` instead of `materialisation` to be consistent with Dagster

### Tests

- Flag any functionality that hasn't been tested (not just "lines uncovered")
- Flag when only the happy path has been tested
- Flag tests that recompute the implementation they are testing
