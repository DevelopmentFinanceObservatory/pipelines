# The layers

A statistical pipeline is built as a stack of pull requests, one per layer, each closing its own issue and based on the one below. Use the layer names verbatim.

Every layer passes the full CI suite on its own and automates what it adds, because each is reviewed and merged on its own. That decides where the cuts fall: a layer that needs a fixture, a seed row or a schema block from a later layer is cut in the wrong place. A pipeline that publishes only nodes needs fewer layers, cut by the same rule.

1. **Extraction step:** the observable source asset and the raw file asset, which writes the download time and the source's release date beside the file.
2. **Raw load step:** the raw table and raw metadata assets, and the CI fixture their tests read. The fixture is real output from the source, narrowed to a small slice that still covers every code the pipeline maps. A hand-built fixture encodes your assumptions about column types and nulls, so the build passes and production does not.
3. **Data cleaning layer:** `sources.yml`, the fixture's block in `scripts/load_ci_fixtures.sql`, the staging model, the `dbt_project.yml` schema block and the dataset's entry in `dbt_ingest/defs.yaml`, together. `dbt build` fails on a `source()` with no table, and `generate_schema_name.sql` raises for a model without `+schema`.
4. **Entity resolution layer:** the resolved model, with only the ontology fixture rows its tests need, and `dbt_utils.equal_rowcount` against staging.
5. **Unified model layer:** the aggregation, the mapping onto Data Commons vocabulary and the StatVar name. Test the aggregation with two rows that differ only in the summed dimension, which collapse to their sum, and a third row in another group that stays separate: row counts and uniqueness tests do not notice a `max()` where a `sum()` belongs. Assert one minted StatVar dcid as a literal string.
6. **DC schema mapping layer:** the `dc_*` models. Where the source has indicator codes, the StatVar model carries every one as `sourceIndicatorCode`, one quoted MCF value per code, so a StatVar minted from several codes keeps them all. For a source with a fixed release calendar, add its release months to `release_calendars` in `dbt_project.yml`, which the provenance model reads for the next release date, and its `completing_release_month` if it publishes partial years.
7. **Documentation:** `transformations.md` and `changelog.md` under `docs/<source>/<dataset>/`, written for the reader described under published text in the skill.
8. **Export to MCF/CSV:** the `ExportSpec` and export asset. Choose `conversion_recipe` deliberately: dfo-apis converts nothing from a provenance without one.
9. **DCP ingestion step:** the ingestion asset, with a test that runs the export and the ingestion together with `ingest_bundle` patched.

## Rebasing the stack

- After a layer's base is amended, rebase with `git rebase --onto <new-parent> <old-base-sha>`. A plain rebase replays the old base commit, and the conflict looks like your code.
- A commit that replaces a file wholesale reverts a lower layer's edits to that file, and the rebase still reports clean. After rebasing, check the lower layer's change survived.
- Close the split by diffing the top of the stack against the branch it was cut from. Anything beyond what you deliberately changed is a slice that lost or duplicated something.
