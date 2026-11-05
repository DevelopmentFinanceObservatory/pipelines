---
name: building-pipelines
description: Build, change or review a data pipeline in DevelopmentFinanceObservatory/pipelines, from a source nobody has ingested yet through to a Data Commons import bundle. Covers the modelling and ontology work the team does before any code, the Dagster asset chain, the stg/int/dc dbt layering, the layered pull request stack, the CI gates, the traps that have cost real time, and how to review a pull request there. Use when adding or changing a pipeline in that repository, or reviewing a pull request that touches one.
---

# Building and reviewing DFO pipelines

Most pipelines here turn a published statistical dataset into a Data Commons import bundle: a `config.json`, an MCF of StatVar definitions, and observation CSVs. Each is one Dagster asset chain, one directory of dbt models, and one export, merged as a stack of small pull requests. Most of this skill describes that shape.

Some pipelines publish only nodes, with no observations, such as the DFO ontology and the agent skills. They follow only part of the pattern: see [Pipelines that publish only nodes](#pipelines-that-publish-only-nodes).

Read `references/layers.md` for what each layer owns, `references/traps.md` before writing code, and `references/verification.md` before claiming anything passes. When reviewing, start at [Reviewing a pull request](#reviewing-a-pull-request).

## Order of work

**Modelling, then ontology, then pipeline.** The team's process puts two steps before the code, and skipping them produces a pipeline that builds green and publishes dcids that resolve to nothing. Every reference this pipeline emits — population type, measurement method, every constraint property, the Source node in the provenance — has to exist in the concept store before a bundle means anything. None of that is enforced by any test, so the build passes either way and the failure appears only after publication.

Before writing code:

1. **A Drive folder and a Pipeline Specification.** Copy the shape of the existing ones. The specification is where the source's grain, its revision behaviour, its known quality problems and its StatVar identity get settled, and it is the artifact the rest of the team reviews.
2. **Ontology notes**, naming every node to mint or widen, with the reasoning. Send these to whoever owns the concept store and let them land first. A node that turns out to need a different shape is cheap to change before the models hardcode it and expensive after.

Then build. If you find yourself deciding modelling questions while writing SQL, stop: that decision belongs in the specification, where someone can disagree with it.

## The asset chain

In a statistical pipeline, each step is a Dagster asset in `src/dfo_pipelines/defs/<source>/<dataset>/assets.py`, named `<source>_<dataset>_<step>`:

```
<source>_<dataset>_source           observable_source_asset, @daily cron
  → <source>_<dataset>_raw_file       downloads or computes, writes a date-partitioned file and its
                                        extraction metadata (downloaded_at, source_release_date)
  → <source>_<dataset>_raw_table      loads the file into DuckDB, overwriting
  → <source>_<dataset>_raw_metadata   loads the extraction metadata, for provenance
  → dbt models via source()
  → <source>_<dataset>_dc_export      ExportSpec fed to export_bundle
  → <source>_<dataset>_dc_ingestion   hands the bundle to ingest_bundle, by hand only
```

The source asset exists so the chain reruns when the publisher republishes rather than on a clock. It must observe something that actually moves with a republish, such as a version token or a release date, never a package version, and it must bypass any cache between it and the publisher.

An asset that reads its upstream's value takes it by parameter name, so Dagster wires `<source>_<dataset>_raw_table(<source>_<dataset>_raw_file: str)` without an import. One that only needs to run after its upstream declares it in `deps`.

Every automated Python asset uses `automation_condition=EAGER_WHEN_CHECKS_PASS` from `defs/automation.py`, and every Python asset check sets `blocking=True`. dbt models get the same condition through `dbt_ingest/defs.yaml`. Plain `eager()` ignores a failed check on the next tick, so bad data flows on.

The ingestion is the exception: it declares no automation condition, because an ingest rewrites the shared graph. It takes the bundle directory the export returns as its value, and depends on `dfo_ontology_dc_ingestion` and `dfo_shared_dc_ingestion`, since the bundle links to their nodes. Ingest those first, and never ingest a bundle whose dcids are not yet defined: the graph accepts it and the references dangle.

## Pipelines that publish only nodes

A pipeline that publishes only nodes keeps what makes any pipeline safe to automate:

- a source asset that observes what moves when the upstream changes, where the upstream can be observed
- a date-partitioned raw copy, kept as the upstream publishes it
- blocking checks and `EAGER_WHEN_CHECKS_PASS`
- a `<source>_<dataset>_dc_export` that writes its own import

It skips what only statistical data needs: the StatVar identity in the specification, the stg/int/dc dbt layering, and the observation CSVs. Its raw step often stores a directory rather than a file, and its export writes MCF nodes directly. Model it on the existing node pipeline closest to it, not on the statistical chain.

## The dbt layering

One directory, `dbt/models/<source>/<dataset>/`, layered:

- `stg_<dataset>.sql` casts the columns the pipeline consumes and nothing else. Narrow `sources.yml` to those columns.
- `int_<dataset>_resolved.sql` resolves source codes to ontology dcids through `ref("code_mapping")`.
- One or more `int_<dataset>_<step>.sql` models reach the published grain and map onto Data Commons vocabulary, minting the StatVar dcid through `mint_statvar_dcid`.
- `dc_<dataset>_statvars.sql`, `dc_<dataset>_observations.sql`, `dc_<dataset>_provenance.sql` are what the export reads.

Every model declares a contract, and every column carries `not_null` unless it can legitimately be null, in which case its description says why. `+schema: <source>_<dataset>` in `dbt_project.yml` is mandatory: `generate_schema_name.sql` raises a compiler error for any model without one.

**One pipeline owns each raw table's staging.** Another pipeline that needs the same source reads that staging model rather than declaring the source again, which dbt refuses. Staging takes everything any consumer needs, and each consumer filters to its own scope further down.

**Every join to `code_mapping` is a left join on purpose.** An unmapped code must produce a null that `not_null` catches, not a row that disappears. Add `dbt_utils.equal_rowcount` against the model each one draws its rows from, or that intent has nothing behind it and an inner join introduced later loses rows silently.

## Published text

Some of a pipeline's text reaches people who use the data and never see the code. The export writes `transformations.md` and `changelog.md` into the provenance node, and a property DFO adds to the graph carries its description to every user of the graph. Write that text for its reader: what the data means, in as few sentences as it takes, naming no SQL file, macro, asset or var. Cutoffs, checks and the logic behind a value go in code comments.

- **A column description** in `models.yml` says how to read the value. How it is computed belongs in the SQL.
- **A source's concepts** take the publisher's own terms, checked against its documents. The OECD, for instance, calls its April figures preliminary and a year only some donors have reported partial, and the two mean different things.
- **A graph property's description** follows the wording of the nearest existing Data Commons property and holds for any graph, so it names no DFO source or process.

## Ship it as a stack

Each pull request covers one issue, so a pipeline is built as one issue per layer. Open an epic issue and the layer issues under it, then a stack of pull requests, each closing its own issue, each based on the one below. `references/layers.md` gives the split, and `.github/pull_request_template.md` sets the body shape.

Every layer must pass the full CI suite standalone, because each is reviewed and merged on its own. That is the constraint that decides where the cuts fall: a layer that needs a fixture, a seed row or a schema block from a later layer is cut in the wrong place.

## Prove things

Two habits separate a pipeline that works from one that reports success.

**Run the real gate.** Run every step in `.github/workflows/ci.yml`, from a script rather than by hand; `references/verification.md` says how. A narrowed `dbt build --select` is not a substitute and is actively misleading.

**Mutate to test a test.** A new test is worth having only if it fails when the behaviour it names is wrong. Break the source, run the test alone, watch it fail, restore, watch it pass. Tests that were never mutated routinely turn out to assert a mock's return value, or an invariant the SQL already guarantees. This is the cheapest review anyone will ever run on this code, and it finds things no reading finds.

## Reviewing a pull request

Apply the review rules in the repository's `AGENTS.md` first, in their order, then these checks specific to pipelines.

1. Check the pull request holds what its layer owns in `references/layers.md`, and nothing that belongs to a later layer.
2. Check it against every trap in `references/traps.md` that touches the files it changes.
3. Verify a finding by running it: a command that fails, a mutation the tests miss, a query that returns the wrong rows. Automated reviewers are often right about the gap and wrong about the premise, so run theirs too.
4. Report blocking findings apart from the rest, each with the concrete failure it causes.

When responding to a review, answer every finding with a fix, a reasoned decline, or why it is a false positive.
