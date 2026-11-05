---
name: building-pipelines
description: Build, change or review a data pipeline in this repository, from a new source through to a Data Commons import bundle. Covers what has to be settled before any code, how a pipeline is split into pull requests, the rules and traps the code does not show, and how to review a pipeline pull request. Use when adding or changing a pipeline, or reviewing a pull request that touches one.
---

# Building and reviewing pipelines

A statistical pipeline turns a published dataset into a Data Commons import bundle: a `config.json`, an MCF of StatVar definitions, and observation CSVs. It is a Dagster asset chain in `src/dfo_pipelines/defs/<source>/<dataset>/`, dbt models in `dbt/models/<source>/<dataset>/`, and an export. Some pipelines publish only nodes, such as `dfo/ontology` and `dfo/skills`, and have no dbt models or observations.

The pipelines follow one pattern closely. Copy the existing pipeline closest to the new source, and take names, layout and wiring from its code.

Read `references/layers.md` when splitting the work into pull requests, and `references/traps.md` before writing code and when reviewing.

## Before building

A pipeline is built from decisions made before any code: the model (StatVars, constraints, provenance) and the build choices (source and dataset names, how a new release is detected, how provenance dates are set, which conversion recipe applies, which quality checks run). If you find yourself deciding a modelling question while writing SQL, stop: it belongs with those decisions, where someone can disagree with it.

Every node the pipeline references, such as the population type, measurement method, each constraint property and the Source node, has to exist in the ontology before the bundle means anything. No test enforces this, so the build passes and publishes dcids that resolve to nothing.

## Rules the code does not show

- **The source asset observes the publisher.** It observes something that changes when the publisher republishes, such as a version token or a release date, and bypasses any cache between it and the publisher, so the chain reruns on a republish. A package version does not qualify.
- **Checks block.** Use the automation conditions in `defs/automation.py` and set `blocking=True` on every asset check. Plain `eager()` ignores a failed check on the next tick, so bad data flows on.
- **Ingestion runs by hand.** An ingest rewrites the shared graph, so the ingestion asset has no automation condition. It depends on `dfo_ontology_dc_ingestion` and `dfo_shared_dc_ingestion`. Ingest those first: the graph accepts a bundle whose dcids are undefined and leaves the references dangling.
- **One pipeline owns each raw table's staging.** Another pipeline that needs the same source reads that staging model. Staging takes everything any consumer needs, and each consumer filters to its own scope further down.
- **Totals reconcile with the source.** Add a singular dbt test that compares the published totals with the source's, so CI repeats the reconciliation.

## Published text

Some of a pipeline's text reaches people who use the data and never see the code. The export writes `transformations.md` and `changelog.md` into the provenance node, and a property DFO adds to the graph carries its description to every user of the graph. Write that text for its reader: what the data means, in as few sentences as it takes, naming no SQL file, macro, asset or var. Cutoffs, checks and the logic behind a value go in code comments.

- **A column description** in `models.yml` says how to read the value. How it is computed belongs in the SQL.
- **A source's concepts** take the publisher's own terms, checked against its documents. The OECD, for instance, calls its April figures preliminary and a year only some donors have reported partial, and the two mean different things.
- **A graph property's description** follows the wording of the nearest existing Data Commons property and holds for any graph, so it names no DFO source or process.
- **The changelog** records changes to the values, the coverage or how the data is modelled, and leaves out changes a user cannot see.

## Reviewing a pull request

Apply the review rules in `AGENTS.md`, then check that the pull request holds what its layer owns in `references/layers.md` and nothing from a later layer, and check it against every trap in `references/traps.md` that touches the files it changes.
