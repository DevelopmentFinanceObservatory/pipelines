-- The table is computed from DAC1 and the WEO, so the source data and the terms
-- they are published under are two URLs each. sourceDataUrl and license hold one
-- string, so they are comma-delimited.
-- FIXME: Emit the URLs as separate values once dcp-tools accepts a list:
-- https://github.com/ONEcampaign/dcp-tools/issues/176
--
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

-- The later of the DAC1 and WEO releases the table is computed from.
source_release as (
    select
        strftime(
            greatest(dac1.source_release_date, weo.source_release_date), '%Y-%m-%d'
        ) as imported_source_release_date
    from {{ source("oecd_dac1", "raw_dac1_metadata") }} as dac1
    cross join {{ source("imf_weo", "raw_weo_metadata") }} as weo
),

observation_range as (
    select
        cast(min(observations.date) as varchar) as earliest_observation_date,
        cast(max(observations.date) as varchar) as latest_observation_date
    from {{ ref("dc_conversion_tables_dac_observations") }} as observations
)

select
    'ONE Conversion Tables (DAC)' as name,
    'source/ONEData' as source,
    'https://docs.one.org/methodologies/data/oda/currency-conversion/' as url,
    'https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/DSD_DAC1@DF_DAC1/,'
    || 'https://data.imf.org/en/datasets/IMF.RES:WEO' as source_data_url,
    'https://www.oecd.org/en/about/terms-conditions.html,'
    || 'https://www.imf.org/external/terms.htm' as license,
    'P6M' as source_release_frequency,
    source_release.imported_source_release_date,
    extraction.last_data_refresh_date,
    {{ var("conversion_tables_deflator_base_year") }} as deflator_base_year,
    observation_range.earliest_observation_date,
    observation_range.latest_observation_date,
    'ONE Conversion Tables (DAC) holds the exchange rates and US-dollar '
    || 'deflators that convert OECD development finance data to other '
    || 'currencies and to constant prices. Deflators and the exchange rates of '
    || 'DAC donor currencies come from the OECD DAC1 table. Every other '
    || 'currency''s rate comes from the IMF World Economic Outlook. Each rate is '
    || 'in units of the currency per US dollar, and each deflator series is '
    || 'rebased to equal 100 in the declared base year.' as description,
    'ONE (' || extraction.refresh_year || '), ONE Conversion Tables (DAC), '
    || 'https://docs.one.org/methodologies/data/oda/currency-conversion/ '
    || '(accessed on ' || extraction.access_date || '), computed from '
    || 'OECD, DAC1: Flows by donor, '
    || 'https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/DSD_DAC1@DF_DAC1/ '
    || 'and IMF, World Economic Outlook, '
    || 'https://data.imf.org/en/datasets/IMF.RES:WEO' as license_attribution
from extraction
cross join source_release
cross join observation_range
