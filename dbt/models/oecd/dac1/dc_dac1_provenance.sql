with metadata as (
    select
        downloaded_at,
        source_release_date,
        cast(extract(year from source_release_date) as string) as release_year
    from {{ source("oecd_dac1", "raw_dac1_metadata") }}
),

extraction as (
    select
        metadata.release_year as source_release_year,
        cast(metadata.source_release_date as string) as imported_source_release_date,
        cast(cast(metadata.downloaded_at as date) as string)
            as access_date,
        {{ next_source_release_date('oecd_dac1', 'metadata.source_release_date') }}
            as next_source_release_date
    from metadata
),

observation_dates as (
    select date from {{ ref("dc_dac1_flow_observations") }}
    union all
    select date from {{ ref("dc_dac1_donor_observations") }}
),

observation_range as (
    select
        cast(min(date) as string) as earliest_observation_date,
        cast(max(date) as string) as latest_observation_date
    from observation_dates
)

select
    'OECD DAC1' as name,
    'dc/s/OrganisationForEconomicCo-operationAndDevelopmentOecd' as source,
    'https://data-explorer.oecd.org/vis?fs[0]=Topic,0%7CDevelopment%23DEV%23&fs[1]=Topic,1%7CDevelopment%23DEV%23%7COfficial%20Development%20Assistance%20%28ODA%29%23DEV_ODA%23&pg=0&fc=Topic&snb=23&df[ds]=dsDisseminateFinalDMZ&df[id]=DSD_DAC1%40DF_DAC1&df[ag]=OECD.DCD.FSD&df[vs]=1.7&dq=._Z.....&pd=,&to[TIME_PERIOD]=false&isAvailabilityDisabled=false&page=0'  -- noqa: LT05
        as url,
    'https://webfs-dcd.oecd.org/files/dotStat/DSD_DAC1/Table1_Data.zip'
        as source_data_url,
    'https://www.oecd.org/en/about/terms-conditions.html' as license,
    'dcid:OECDTermsAndConditions' as license_type,
    'P3M' as source_release_frequency,
    extraction.imported_source_release_date,
    extraction.next_source_release_date,
    extraction.next_source_release_date as next_data_refresh_date,
    observation_range.earliest_observation_date,
    observation_range.latest_observation_date,
    '{{ dbt.escape_single_quotes(config.get("meta").dataset_description) }}' as description,
    'OECD (' || extraction.source_release_year || '), OECD DAC1, '
    || 'OECD Data Explorer, '
    || 'https://webfs-dcd.oecd.org/files/dotStat/DSD_DAC1/Table1_Data.zip'
    || ' (accessed on ' || extraction.access_date || ')'
        as license_attribution
from extraction
cross join observation_range
