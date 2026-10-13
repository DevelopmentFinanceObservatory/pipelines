-- lastDataRefreshDate is the UTC timestamp of the dbt run that computed the
-- table, so every rebuild, including a same-day correction, moves it. The API's
-- rate-table cache keys on it. The citation's access date is that run's day.
with extraction as (
    select
        '{{ run_started_at.strftime("%Y-%m-%dT%H:%M:%SZ") }}'
            as last_data_refresh_date,
        '{{ run_started_at.strftime("%Y-%m-%d") }}' as access_date,
        '{{ run_started_at.strftime("%Y") }}' as refresh_year
),

source_release as (
    select strftime(source_release_date, '%Y-%m-%d') as source_release_date
    from {{ source("imf_weo", "raw_weo_metadata") }}
),

observation_range as (
    select
        cast(min(observations.date) as varchar) as earliest_observation_date,
        cast(max(observations.date) as varchar) as latest_observation_date
    from {{ ref("dc_conversion_tables_imf_observations") }} as observations
)

select
    'ONE Conversion Tables (IMF)' as name,
    'source/ONEData' as source,
    'https://docs.one.org/methodologies/data/oda/currency-conversion/' as url,
    'https://data.imf.org/en/datasets/IMF.RES:WEO' as source_data_url,
    'https://www.imf.org/external/terms.htm' as license,
    'P6M' as source_release_frequency,
    source_release.source_release_date,
    extraction.last_data_refresh_date,
    {{ var("conversion_tables_deflator_base_year") }} as deflator_base_year,
    observation_range.earliest_observation_date,
    observation_range.latest_observation_date,
    'ONE Conversion Tables (IMF) holds the exchange rates and US-dollar '
    || 'deflators that convert IMF and World Bank data to other currencies and '
    || 'to constant prices, computed from the IMF World Economic Outlook. Each '
    || 'currency''s rate is national-currency GDP divided by US-dollar GDP, and '
    || 'each country''s deflator is the GDP deflator multiplied by US-dollar GDP '
    || 'over national-currency GDP, rebased to equal 100 in the declared base '
    || 'year. Years after a country''s last actual year are IMF projections.'
        as description,
    'ONE (' || extraction.refresh_year || '), ONE Conversion Tables (IMF), '
    || 'https://docs.one.org/methodologies/data/oda/currency-conversion/ '
    || '(accessed on ' || extraction.access_date || '), computed from '
    || 'IMF, World Economic Outlook, '
    || 'https://data.imf.org/en/datasets/IMF.RES:WEO' as license_attribution
from extraction
cross join source_release
cross join observation_range
