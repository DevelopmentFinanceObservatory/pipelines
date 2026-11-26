# Traps

Each of these has cost real time on a pipeline that was otherwise green. Read them before writing code, and check a pull request against them when reviewing.

## Extraction and raw load

### A check must be able to fail

Count what the check is about: rows, not bytes. An empty parquet file still carries a schema and a footer, so a size check passes on no data. Never set a threshold from the size of the current extract.

### Raise, or block

Raise when the asset cannot be produced, for example when the download fails or the upstream is malformed. Use a blocking check when the asset was produced but must not flow on, for example when it is empty. A non-blocking check lets an empty extract replace a good table on a green run.

### The download fetches what the observation saw

The source asset and the landing asset must agree on the version and bypass the same caches. Otherwise the pipeline observes a new release and materializes the old one, recorded against the new version.

### A download stays as published

Rename, cast and fix in the raw table load or in staging, never in the download. DuckDB ignores identifier case, so columns whose names differ only in case, such as a code column and its label, collide on load: aliasing them by name binds both aliases to the same column. Alias them by position.

### Read the upstream's value, do not rebuild it

A downstream asset reads its upstream's path from the value the upstream returned, not by rebuilding it from the resource.

### A landing directory is written once

`build_landing_asset` writes each download to a new directory, writes `metadata.json` last, and removes a directory whose write failed. `build_raw_table_assets` refuses a directory without `metadata.json`, as incomplete.

### A fetch writes through the landing `UPath`

The landing directory may be on an object store, so a fetch writes each declared file through the `UPath` it is given, with its methods or a handle opened from it. A library that takes only local paths gets neither the `UPath` nor `str(path)`: it writes to a local `tempfile.TemporaryDirectory()`, which also holds every working file, and the fetch copies the declared file across.

### An export directory is written once

Each export writes its bundle to a new directory through `write_once_directory`, which removes it if the run raises or is cancelled. Ingestion reads only the directory a successful materialization recorded, so one left by a crash is never ingested. Enter the `with` just before the first write, and keep every write and read of the bundle inside it.

## dbt

### A narrowed dbt build is a fail-open gate

`dbt build --select +path:models/<source>/<dataset>` on a fresh database fails on other pipelines' tests whatever your code does. dbt's default indirect test selection is eager, so selecting `code_mapping` as an upstream pulls in every test that refers to it, including tests on models outside the selection that never get built. The run fails on models you did not touch, which trains you to read failures as noise.

### Left joins are load-bearing and unguarded by default

Every join that resolves a source row's codes through `code_mapping` is a left join, so an unmapped code yields a null that `not_null` catches. Nothing enforces that: an inner join drops the unmapped rows instead, and every test still passes. Add `dbt_utils.equal_rowcount` against the upstream model. It catches fan-out too, from any mapping with more than one row per code.

### Filters and NULLs hide what they drop

A filter listing today's values silently drops any value the publisher adds later, so pair it with an `accepted_values` test on the column it filters.

`concat()` skips NULLs, so a dcid built with it is never null and its `not_null` test is vacuous; `||` propagates them. A `coalesce` hides an unmapped value along with a missing one, so apply it only where a missing value has a meaning.

### Types and rounding

Cast a source value to `decimal(38,12)` in staging when the pipeline sums it, because a sum of doubles depends on row order. Otherwise keep the source's `double`. A narrower cast before the last sum, such as `decimal(18,5)` on a source reporting in millions, rounds every row to ten dollars, so aggregates lose value and small amounts publish as zero.

Round once, after the last sum, in the model that sets the unit. Every published value is a `double` through the `round_significant` macro, which keeps the 15 significant digits a Data Commons value carries (its API serves float64). Round currency amounts to cents first.

Guard it with a singular test that reconciles each year's published total with the source to within half a cent per observation. A unit test cannot: dbt casts the expected rows to the model's own output types, so a lossy cast rounds both sides alike.

### A seed only tests read is no model's parent

A seed referenced only from tests, such as a codelist for a relationship test, is not a parent of any asset, so on a fresh environment automation runs the tests before the seed exists and they error. Validate codes where they resolve, through `code_mapping`, or declare the dependency in the model with `-- depends_on: {{ ref(...) }}`.

### `union all by name` is strict on BigQuery

Deployed runs use BigQuery. DuckDB fills a missing column with NULL; BigQuery refuses. Select every column on both sides, casting NULL where one side has none.

## Data Commons export

### Reuse base nodes, and check before minting

A dcid that nothing defines still ingests: the graph has no referential integrity and turns it into a placeholder node. Before minting a node, check whether Data Commons already has one, and reuse it.

Nothing tests the dcids the models write as literals, such as a measured property, a unit or a provenance's source. Before ingesting, check each against the compiled ontology, the shared nodes and base Data Commons.

### Every entity column is not null

Data Commons assigns an observation's entities by sorting their property names, so a blank entity shifts the next one into its slot and the import succeeds with the wrong data. Test every entity column with `not_null`.

### Each import owns its provenance

An ingest deletes every edge carrying the import's provenance, so a file attributed to the wrong provenance is deleted by another pipeline's refresh.

### Encode free text in MCF

MCF has no escape for a quote, so a quoted value ends at the next quote, and dcp-tools deletes line breaks from every value. Encode free text with `escape_markdown` in `data_commons/mcf.py`, and test the encoding with quotes, a trailing backslash and several lines.

### Mint the dcid once

Mint the StatVar dcid in one model and carry it through. Joining back on the columns it was minted from, or parsing it apart again, breaks as soon as one of them changes.

### measurementMethod participates in no dcid

`mint_statvar_dcid` never reads it, so a measurement method belongs on the observation, not in the StatVar's identity. Keeping it off the identity means the series is not reminted when the production method changes.

Every property column the observation CSV carries must appear in the `property_to_column` of its `ObservationInput` in `ExportSpec.observation_inputs`, measurementMethod included, or dcp-tools drops it from the bundle without error.

## CI

### The ontology compiler never runs in CI

`scripts/load_ci_fixtures.sql` builds `dfo_ontology.raw_code_mapping` from a committed CSV, not from a compile. A pinned ontology revision that can no longer compile is therefore invisible in CI: every pipeline keeps resolving against the committed snapshot, and nothing fails until someone refreshes the ontology.
