# AGENTS.md

## Code Review Rules

This section covers how to review a pull request in this repo.

- Verify all findings before commenting
- Drop low-priority findings that will be made irrelevant by higher-priority findings

The following sections are in roughly priority order.

### Scope

- Start by reading the issue that the PR is addressing
- The PR should address a single issue, flag if it combines multiple or has added scope

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
