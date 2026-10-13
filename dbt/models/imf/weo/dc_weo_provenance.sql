with metadata as (
    select
        source_release_date,
        extract(year from source_release_date) as release_year,
        extract(month from source_release_date) as release_month
    from {{ source("imf_weo", "raw_weo_metadata") }}
),

extraction as (
    select
        strftime(metadata.source_release_date, '%Y-%m-%d') as source_release_date,
        -- The IMF publishes WEO in April and October.
        case
            when metadata.release_month < 4
                then cast(metadata.release_year as varchar) || '-04'
            when metadata.release_month < 10
                then cast(metadata.release_year as varchar) || '-10'
            else cast(metadata.release_year + 1 as varchar) || '-04'
        end as next_source_release_date
    from metadata
),

observation_range as (
    select
        cast(min(observations.date) as varchar) as earliest_observation_date,
        cast(max(observations.date) as varchar) as latest_observation_date
    from {{ ref("dc_weo_observations") }} as observations
)

select
    'IMF World Economic Outlook (WEO)' as name,
    weo_source.dcid as source,
    'https://data.imf.org/en/datasets/IMF.RES:WEO' as url,
    'https://data.imf.org/en/datasets/IMF.RES:WEO' as source_data_url,
    'https://www.imf.org/en/about/copyright-and-terms' as license,
    'dcid:InternationalMonetaryFundCopyrightAndUsage' as license_type,
    'P6M' as source_release_frequency,
    extraction.source_release_date,
    extraction.next_source_release_date,
    extraction.next_source_release_date as next_data_refresh_date,
    observation_range.earliest_observation_date,
    observation_range.latest_observation_date,
    'The World Economic Outlook (WEO) database holds the IMF''s macroeconomic '
    || 'data and projections for its member economies and country groups. It '
    || 'covers national accounts, inflation, unemployment, the balance of '
    || 'payments, government revenue, spending and debt, trade and commodity '
    || 'prices, with data from 1980 and projections for most series five years '
    || 'ahead.' as description,
    'Source: International Monetary Fund, World Economic Outlook Database, '
    || 'https://data.imf.org/en/datasets/IMF.RES:WEO' as license_attribution
from extraction
cross join observation_range
cross join {{ ref("weo_source") }} as weo_source
