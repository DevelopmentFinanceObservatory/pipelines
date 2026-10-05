# Pipeline documentation

User-facing documentation for each dataset the platform publishes.

One directory per dataset, at `<source>/<dataset>`, matching the Dagster asset
group, the dbt schema and the file store prefix for the same pipeline. Each dataset
has two pages:

- `transformations.md` — a plain-language explanation of what the pipeline does to
  the source data, always describing the current state of the dataset. Rewritten
  in place.
- `changelog.md` — dated entries for every production release that changes how the
  data is processed. Append-only, newest first.
