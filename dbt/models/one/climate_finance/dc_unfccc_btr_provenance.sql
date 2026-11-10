-- The totals are read from a climate-finance package release, which takes them
-- from the parties' Biennial Transparency Report workbooks, so the imported
-- release date is that release's. The release is cut by hand, so the node
-- carries no next release date. The UNFCCC terms have no licence type node.
with metadata as (
    select strftime(source_release_date, '%Y-%m-%d') as imported_source_release_date
    from {{ source("one_climate_finance", "raw_climate_finance_metadata") }}
),

observation_range as (
    select
        cast(min(observations.date) as varchar) as earliest_observation_date,
        cast(max(observations.date) as varchar) as latest_observation_date
    from {{ ref("dc_unfccc_btr_observations") }} as observations
)

select
    'UNFCCC Biennial Transparency Reports' as name,
    unfccc_source.dcid as source,
    'https://unfccc.int/biennial-transparency-reports' as url,
    'https://unfccc.int/first-biennial-transparency-reports' as source_data_url,
    'https://unfccc.int/this-site/terms-of-use' as license,
    -- Parties submit a Biennial Transparency Report every two years.
    'P2Y' as source_release_frequency,
    metadata.imported_source_release_date,
    observation_range.earliest_observation_date,
    observation_range.latest_observation_date,
    'Climate finance each party reports to the UNFCCC in its Biennial '
    || 'Transparency Report. Each party works out its climate amounts itself, '
    || 'with its own coefficients or by assessing each project. The DFO takes '
    || 'the bilateral totals of Table III.1 by party, year and status.'
        as description,
    'Source: UNFCCC, Biennial Transparency Reports, '
    || 'https://unfccc.int/biennial-transparency-reports' as license_attribution
from metadata
cross join observation_range
cross join {{ ref("unfccc_source") }} as unfccc_source
