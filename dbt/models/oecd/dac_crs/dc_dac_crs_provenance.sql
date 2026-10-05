with metadata as (
    select
        downloaded_at,
        source_release_date,
        cast(extract(year from source_release_date) as varchar) as release_year
    from {{ source("oecd_dac_crs", "raw_dac_crs_metadata") }}
),

extraction as (
    select
        metadata.release_year as source_release_year,
        strftime(metadata.downloaded_at at time zone 'UTC', '%Y-%m-%d')
            as last_data_refresh_date,
        {{ next_crs_release_date('metadata.source_release_date') }}
            as next_source_release_date
    from metadata
),

observation_range as (
    select
        cast(min(observations.date) as varchar) as earliest_observation_date,
        cast(max(observations.date) as varchar) as latest_observation_date
    from {{ ref("dc_dac_crs_observations") }} as observations
)

select
    'OECD Creditor Reporting System (CRS)' as name,
    'dc/s/OrganisationForEconomicCo-operationAndDevelopmentOecd' as source,
    'https://data-explorer.oecd.org/vis?tm=crs&pg=0&snb=30&df[ds]=dsDisseminateFinalDMZ&df[id]=DSD_CRS%40DF_CRS&df[ag]=OECD.DCD.FSD&df[vs]=1.6&isAvailabilityDisabled=false&page=0'  -- noqa: LT05
        as url,
    'https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS.parquet' as source_data_url,
    'https://www.oecd.org/en/about/terms-conditions.html' as license,
    'dcid:OECDTermsAndConditions' as license_type,
    'P3M' as source_release_frequency,
    extraction.last_data_refresh_date,
    extraction.next_source_release_date,
    extraction.next_source_release_date as next_data_refresh_date,
    observation_range.earliest_observation_date,
    observation_range.latest_observation_date,
    'The Creditor Reporting System (CRS) is the OECD Development Assistance '
    || 'Committee''s activity-level database of development finance. Each record '
    || 'is a project, programme or contribution reported by a bilateral provider, '
    || 'a multilateral organisation or a private foundation, with its recipient, '
    || 'sector and purpose, channel of delivery, aid type, and commitment and '
    || 'disbursement amounts. It covers ODA, other official flows, private sector '
    || 'instruments and private philanthropy, and is the source for any breakdown '
    || 'by sector, channel or project.' as description,
    'OECD (' || extraction.source_release_year || '), '
    || 'Creditor Reporting System (CRS), OECD Data Explorer, '
    || 'https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS.parquet'
    || ' (accessed on ' || extraction.last_data_refresh_date || ')'
        as license_attribution
from extraction
cross join observation_range
