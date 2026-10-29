with metadata as (
    select
        downloaded_at,
        source_release_date,
        cast(extract(year from source_release_date) as varchar) as release_year
    from {{ source("oecd_dac2a", "raw_dac2a_metadata") }}
),

extraction as (
    select
        metadata.source_release_date,
        metadata.release_year as source_release_year,
        strftime(metadata.source_release_date, '%Y-%m-%d') as imported_source_release_date,
        strftime(metadata.downloaded_at at time zone 'UTC', '%Y-%m-%d')
            as access_date,
        {{ next_source_release_date('oecd_dac2a', 'metadata.source_release_date') }}
            as next_source_release_date
    from metadata
),

observation_range as (
    select
        cast(min(date) as varchar) as earliest_observation_date,
        cast(max(date) as varchar) as latest_observation_date
    from {{ ref("dc_dac2a_observations") }}
)

select
    'OECD DAC2A' as name,
    'dc/s/OrganisationForEconomicCo-operationAndDevelopmentOecd' as source,
    'https://data-explorer.oecd.org/vis?tm=dac2a&pg=0&snb=1&df[ds]=dsDisseminateFinalDMZ&df[id]=DSD_DAC2%40DF_DAC2A&df[ag]=OECD.DCD.FSD&df[vs]=1.6&dq=.DPGC.206.USD.Q&lom=LASTNPERIODS&lo=5&to[TIME_PERIOD]=false&isAvailabilityDisabled=false&page=0'  -- noqa: LT05
        as url,
    'https://webfs-dcd.oecd.org/files/dotStat/DSD_DAC2/Table2a_Data.zip'
        as source_data_url,
    'https://www.oecd.org/en/about/terms-conditions.html' as license,
    'dcid:OECDTermsAndConditions' as license_type,
    'P3M' as source_release_frequency,
    extraction.imported_source_release_date,
    extraction.next_source_release_date,
    extraction.next_source_release_date as next_data_refresh_date,
    observation_range.earliest_observation_date,
    observation_range.latest_observation_date,
    {{
        partial_observation_date(
            'oecd_dac2a',
            'extraction.source_release_date',
            'observation_range.latest_observation_date'
        )
    }} as partial_observation_date,
    'DAC2A is the OECD Development Assistance Committee''s table of ODA '
    || 'disbursements by donor and recipient country or region. It covers '
    || 'bilateral donors and multilateral agencies reporting their own '
    || 'outflows, and includes the OECD''s estimate of each donor''s core '
    || 'multilateral contributions attributed to recipients.' as description,
    'OECD (' || extraction.source_release_year || '), OECD DAC2A, '
    || 'OECD Data Explorer, '
    || 'https://webfs-dcd.oecd.org/files/dotStat/DSD_DAC2/Table2a_Data.zip'
    || ' (accessed on ' || extraction.access_date || ')'
        as license_attribution
from extraction
cross join observation_range
