with metadata as (
    select
        downloaded_at,
        source_release_date,
        cast(extract(year from source_release_date) as varchar) as release_year
    from {{ source("oecd_dac2b", "raw_dac2b_metadata") }}
),

extraction as (
    select
        metadata.release_year as source_release_year,
        strftime(metadata.source_release_date, '%Y-%m-%d') as imported_source_release_date,
        strftime(metadata.downloaded_at at time zone 'UTC', '%Y-%m-%d')
            as access_date,
        {{ next_source_release_date('oecd_dac2b', 'metadata.source_release_date') }}
            as next_source_release_date
    from metadata
),

observation_range as (
    select
        cast(min(date) as varchar) as earliest_observation_date,
        cast(max(date) as varchar) as latest_observation_date
    from {{ ref("dc_dac2b_observations") }}
)

select
    'OECD DAC2B' as name,
    'dc/s/OrganisationForEconomicCo-operationAndDevelopmentOecd' as source,
    'https://data-explorer.oecd.org/vis?fs[0]=Topic,1%7CDevelopment%23DEV%23%7COfficial%20Development%20Assistance%20%28ODA%29%23DEV_ODA%23&fs[1]=Topic,2%7CDevelopment%23DEV%23%7COfficial%20Development%20Assistance%20%28ODA%29%23DEV_ODA%23%7CFlows%20by%20provider%20and%20recipient%23DEV_ODA_FPR%23&pg=0&fc=Topic&snb=8&df[ds]=dsDisseminateFinalDMZ&df[id]=DSD_DAC2%40DF_DAC2B&df[ag]=OECD.DCD.FSD&dq=DAC%2BAUS%2BAUT%2BBEL%2BCAN%2BCZE%2BDNK%2BFIN%2BFRA%2BDEU%2BGRC%2BITA%2BJPN%2BKOR%2BLUX%2BNLD%2BNZL%2BNOR%2BPOL%2BPRT%2BSVK%2BSVN%2BESP%2BSWE%2BCHE%2BGBR%2BUSA.DPGC..USD.Q&lom=LASTNPERIODS&lo=5&to[TIME_PERIOD]=false'  -- noqa: LT05
        as url,
    'https://webfs-dcd.oecd.org/files/dotStat/DSD_DAC2/Table2b_Data.zip'
        as source_data_url,
    'https://www.oecd.org/en/about/terms-conditions.html' as license,
    'dcid:OECDTermsAndConditions' as license_type,
    'P3M' as source_release_frequency,
    extraction.imported_source_release_date,
    extraction.next_source_release_date,
    extraction.next_source_release_date as next_data_refresh_date,
    observation_range.earliest_observation_date,
    observation_range.latest_observation_date,
    'DAC2B is the OECD Development Assistance Committee''s table of other '
    || 'official flows and officially supported export credits by donor and '
    || 'recipient. It covers non-concessional official lending, grants and '
    || 'equity outside ODA. Bilateral donors report mostly against regional and '
    || 'income-group aggregates, so country-level detail comes largely from '
    || 'multilateral lenders.' as description,
    'OECD (' || extraction.source_release_year || '), OECD DAC2B, '
    || 'OECD Data Explorer, '
    || 'https://webfs-dcd.oecd.org/files/dotStat/DSD_DAC2/Table2b_Data.zip'
    || ' (accessed on ' || extraction.access_date || ')'
        as license_attribution
from extraction
cross join observation_range
