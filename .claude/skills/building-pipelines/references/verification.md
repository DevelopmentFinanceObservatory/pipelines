# Verification

## Run every gate, from a script

`.github/workflows/ci.yml` defines what must pass. Read it at the start of the work and write a script that runs every step of every job, then run that script for every layer of the stack. Assembling the steps by hand each time is how one goes missing, and a missing gate is invisible: the run reports clean.

Point the script at a DuckDB file of its own for each worktree, deleted before each run, so a stale table cannot make a build pass that should fail.

Run it yourself before committing. An agent that reports every gate passed is reporting what a script printed, and confirming costs a minute.

## Read what a gate is really telling you

`dbt build` reports `WARN` separately from `ERROR`. A warning that appears at a particular layer and not before is information about that layer, not background noise: it locates which pull request introduced it and lets that pull request explain it.

Coverage percentages and test counts are not evidence that a test is meaningful. They count tests that exist.

## Test a test by breaking the code

For each test worth having, make the smallest change to the source that would make its claim false, run that test alone, and watch it fail. Restore, and watch it pass. Restore between mutations so they do not mask each other.

This is not ceremony. Applied to a pipeline whose suite was entirely green, it found:

- an export mapping where swapping two properties, or deleting one, left every test passing
- a unit test that could not detect a constant becoming a per-row branch, because its fixture had one row
- a unit test duplicating its neighbour's branch and expected value, asserting nothing new
- two guard tests that caught a regression only by falling through to a 1.1 GB download
- a resolved model where an inner join dropped a third of the rows with zero failures reported

None of those is visible by reading. All five were visible in one mutation each.

An enforced contract stops a mutation before the test runs: the model fails to build, and the test reads the stale table and passes. Mutate the whole earlier state, contracts included, and rebuild everything downstream before running the test.

Derive any expectation that comes from a fixture, such as its size or row count, from the fixture itself. A hard-coded one breaks the first time the fixture changes.

## Distinguish the three kinds of test

When auditing, classify rather than count:

- **Catches** — can fail against the real fixture, and does fail when its claim is broken.
- **Structural** — guaranteed true by the SQL. A `not_null` on a column set from a literal, or uniqueness a `group by` already enforces. Worth keeping as a contract, worth naming honestly, not worth counting as coverage.
- **Vacuous** — structurally guaranteed *and* masking a real gap, because its name promises something it cannot check.

A suite of seventy tests where forty catch is in better shape than one where the ratio is unknown.

## Verify an end-to-end claim yourself

Run the export and read what it wrote. Check the row counts, a sample dcid, the scaling, and that every property the spec maps appears in the CSV. A test that builds its own fixture tables through raw SQL proves the export mechanics and nothing about whether the models produce that shape.

Reconcile the exported total against the source, and keep the reconciliation as a singular dbt test so CI repeats it. A total that is close but not equal is worth understanding before shipping.
