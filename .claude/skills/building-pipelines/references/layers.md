# The layers

Use the layer names below verbatim: a reviewer recognises "entity resolution layer" and will not recognise a name you invented.

These layers are for a statistical pipeline. A pipeline that publishes only nodes has no dbt models, so it needs fewer layers, cut by the same rule.

Each layer automates what it adds, and adds its assets and models to automation, so every merged layer runs on its own.

Each layer is one issue and one pull request.

## 1. Extraction step

The dependency, the observable source asset, the raw file asset, and their automation tests. The source asset is the pipeline's new-data check. The raw file asset writes the extraction metadata, the download time and the source's release date, beside the file it downloads.

## 2. Raw load step

The raw table asset, the raw metadata asset that loads the extraction metadata into DuckDB, and the committed CI fixture their tests read. The fixture lands with the first test that reads it, so if an extraction step test uses it as the download, it lands there instead.

The fixture is real output from the source, narrowed to a small slice that still covers every code the pipeline maps, not hand-built. A hand-built fixture encodes your assumptions about the source's column types and nulls, so the build passes and production does not.

## 3. Data cleaning layer

`sources.yml`, the block in `scripts/load_ci_fixtures.sql` that loads the fixture into DuckDB, `stg_<dataset>.sql`, the `dbt_project.yml` schema block, the staging entry in `models.yml`, and the dataset's `[source, dataset]` entry in `src/dfo_pipelines/defs/dbt_ingest/defs.yaml`.

The loader block lands with `sources.yml`, because `dbt build` fails outright when a `source()` names a table that does not exist. The `dbt_project.yml` block has to land here, with the first model under that directory, because `generate_schema_name.sql` raises for any model without a `+schema`. The `defs.yaml` entry lands here too, so the dbt models automate from the first one, and each later layer's models inherit it.

## 4. Entity resolution layer

`int_<dataset>_resolved.sql`, its `models.yml` entry, the first unit test case, and the ontology fixture rows the resolution needs.

Only the rows this layer's `not_null` tests need. Rows for a code system that a later layer resolves belong to that later layer.

Add `dbt_utils.equal_rowcount` against the staging model here.

## 5. Unified model layer

The aggregation, the mapping onto Data Commons vocabulary, the StatVar name macro, their `models.yml` entries, unit tests, and the remaining ontology fixture rows.

Test the aggregation itself. Two input rows differing only in the dimension being summed away, collapsing to one row holding their sum, and a third row in another group that stays separate. Row counts and uniqueness checks do not notice a `max()` where a `sum()` belongs.

A unit test asserting the minted StatVar dcid as a literal string belongs here. It is the one thing the identity decision exists to produce.

## 6. DC schema mapping layer

The `dc_*` models and the rest of the unit tests.

The provenance model reads the metadata table from layer 2 and is where the Source dcid, the license and the release calendar live.

The StatVar model carries the source's indicator codes as `sourceIndicatorCode`, one quoted MCF value per code, so a StatVar minted from several codes keeps them all.

## 7. Documentation

`transformations.md` and `changelog.md` under `docs/<source>/<dataset>/`. The export writes both into the provenance node, so they reach users with the data: describe what the data is and how it was transformed, for someone who never saw the code. The changelog records changes to the values, the coverage or how the data is modelled, never plumbing a user cannot see.

## 8. Export to MCF/CSV

The `ExportSpec` and export asset with its automation test. The export returns the bundle directory as its value. `conversion_recipe` has no default: set a recipe or `None` explicitly, since dfo-apis converts nothing from a provenance without one.

## 9. DCP ingestion step

The ingestion asset, a test that runs the export and the ingestion together with `ingest_bundle` patched and checks the bundle path and import name it receives, and a docstring saying it runs by hand.

## Mechanics

Create branches bottom-up, each from the one below, layer 1 from `origin/dev`. Verify each layer standalone before building the next: a layer that only passes with a later layer's files is cut in the wrong place, and you want to know at once.

When a layer's base has been amended, `git rebase <new-parent>` replays the old base commit and produces a conflict that looks like your code. Use `git rebase --onto <new-parent> <old-base-sha>`.

A commit that replaces a file wholesale silently reverts edits a lower layer made to that file, and the rebase still reports clean. After rebasing, confirm the lower layer's change is still there.

The check that closes the split: a diff between the top of the stack and the single branch you cut it from should contain only what you deliberately changed since. Anything else is a slice that lost or duplicated something.
