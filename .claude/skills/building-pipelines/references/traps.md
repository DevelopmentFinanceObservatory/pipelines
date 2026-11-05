# Traps

Each of these has cost real time on a pipeline that was otherwise green. Read them before writing code, and check a pull request against them when reviewing.

## Extraction and raw load

### A check must be able to fail

Count what the check is about: rows, not bytes. An empty parquet file still carries a schema and a footer, so a size check passes on no data. Never set a threshold from the size of the current extract.

### Raise, or block

Raise when the asset cannot be produced, for example when the download fails or the upstream is malformed. Use a blocking check when the asset was produced but must not flow on, for example when it is empty. A non-blocking check lets an empty extract replace a good table on a green run.

### The download fetches what the observation saw

The source asset and the raw file asset must agree on the version and bypass the same caches. Otherwise the pipeline observes a new release and materializes the old one, recorded against the new version.

### The raw file stays as published

Rename, cast and fix in the raw table load or in staging, never in the raw file. DuckDB ignores identifier case, so columns whose names differ only in case, such as a code column and its label, collide on load: aliasing them by name binds both aliases to the same column. Alias them by position.

### Read the upstream's value, do not rebuild it

A downstream asset reads its upstream's path from the value the upstream returned, not by rebuilding it from the resource.

### Artifacts are replaced in place

Date-partitioned paths are keyed to the day rather than the run. Clear the partition before writing, or a same-day rerun republishes files an earlier run wrote and this one did not. A failure partway leaves the partition incomplete until the next successful run.

## dbt

### A narrowed dbt build is a fail-open gate

`dbt build --select +path:models/<source>/<dataset>` on a fresh database fails on other pipelines' tests whatever your code does. dbt's default indirect test selection is eager, so selecting `code_mapping` as an upstream pulls in every test that refers to it, including tests on models outside the selection that never get built. The run fails on models you did not touch, which trains you to read failures as noise.

CI runs a bare `dbt build` with no `--select`. Run that. Narrowing needs `--indirect-selection=cautious` to mean anything, and even then proves less.

### Left joins are load-bearing and unguarded by default

Every join to `code_mapping` is a left join so an unmapped code yields a null that `not_null` catches. Nothing enforces that: an inner join drops the unmapped rows instead, and every test still passes. Add `dbt_utils.equal_rowcount` against the upstream model. It catches fan-out too, from any mapping with more than one row per code.

### Filters and NULLs hide what they drop

A filter listing today's values silently drops any value the publisher adds later, so pair it with an `accepted_values` test on the column it filters.

`concat()` skips NULLs, so a dcid built with it is never null and its `not_null` test is vacuous; `||` propagates them. A `coalesce` hides an unmapped value along with a missing one, so apply it only where a missing value has a meaning.

### Round once, after the sum

A source reporting in millions, cast to `decimal(18,5)` in staging, rounds every row to ten dollars before it is summed, so aggregates lose value and small amounts publish as zero. Keep full precision, `decimal(38,12)`, from staging through aggregation and round only in the observation model.

Guard it with a singular test that reconciles each year's published total with the source to within half a cent per observation. A unit test cannot: dbt casts the expected rows to the model's own output types, so a lossy cast rounds both sides alike.

### A seed only tests read is no model's parent

A seed referenced only from tests, such as a codelist for a relationship test, is not a parent of any asset, so on a fresh environment automation runs the tests before the seed exists and they error. Validate codes where they resolve, through `code_mapping`, or declare the dependency in the model with `-- depends_on: {{ ref(...) }}`.

### `union all by name` is strict on BigQuery

DuckDB fills a missing column with NULL; BigQuery refuses. Select every column on both sides, casting NULL where one side has none.

## Data Commons export

### Reuse base nodes, and check before minting

A dcid that nothing defines still ingests: the graph has no referential integrity and turns it into a placeholder node. Before minting a node, check whether Data Commons already has one, and reuse it.

Nothing tests the dcids the models write as literals, such as a measured property, a unit or a provenance's source. Before ingesting, check each against the compiled ontology, the shared nodes and base Data Commons.

### Every entity column is not null

Data Commons assigns an observation's entities by sorting their property names, so a blank entity shifts the next one into its slot and the import succeeds with the wrong data. Test every entity column with `not_null`.

### Each import owns its name and its provenance

`importName` decides where the bundle is stored, so two imports sharing one overwrite each other. An ingest deletes every edge carrying the import's provenance, so a file attributed to the wrong provenance is deleted by another pipeline's refresh.

### Encode free text in MCF

MCF has no escape for a quote, so a quoted value ends at the next quote, and dcp-tools deletes line breaks from every value. Encode free text before writing it, and test the encoding with quotes, a trailing backslash and several lines.

### Mint the dcid once

Mint the StatVar dcid in one model and carry it through. Joining back on the columns it was minted from, or parsing it apart again, breaks as soon as one of them changes.

### measurementMethod participates in no dcid

`mint_statvar_dcid` never reads it, so a measurement method belongs on the observation, not in the StatVar's identity. Keeping it off the identity means the series is not reminted when the production method changes.

Every property column the observation CSV carries must appear in the `property_to_column` of its `ObservationInput` in `ExportSpec.observation_inputs`, measurementMethod included, or dcp-tools drops it from the bundle without error.

## CI

### The ontology compiler never runs in CI

`scripts/load_ci_fixtures.sql` builds `dfo_ontology.raw_code_mapping` from a committed CSV, not from a compile. A pinned ontology revision that can no longer compile is therefore invisible in CI: every pipeline keeps resolving against the committed snapshot, and nothing fails until someone refreshes the ontology.
