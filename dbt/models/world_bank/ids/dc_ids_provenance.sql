-- The World Bank splits IDS across six bulk files by debtor name, so the source
-- data is six URLs. sourceDataUrl holds one string, so they are comma-delimited;
-- the same list drives the download in the IDS assets.
-- FIXME: Emit the six URLs as separate values once dcp-tools accepts a list:
-- https://github.com/ONEcampaign/dcp-tools/issues/176
with bulk_files as (
    select array_to_string(
        [
            'https://datacatalogfiles.worldbank.org/ddh-published/0038015/DR0092201/A_D.xlsx',
            'https://datacatalogfiles.worldbank.org/ddh-published/0038015/DR0092202/E_K.xlsx',
            'https://datacatalogfiles.worldbank.org/ddh-published/0038015/DR0092203/L_M.xlsx',
            'https://datacatalogfiles.worldbank.org/ddh-published/0038015/DR0093225/N_Q.xlsx',
            'https://datacatalogfiles.worldbank.org/ddh-published/0038015/DR0092204/R_U.xlsx',
            'https://datacatalogfiles.worldbank.org/ddh-published/0038015/DR0092205/V_Z.xlsx'
        ],
        ','
    ) as source_data_url
),

metadata as (
    select
        downloaded_at,
        cast(extract(year from source_release_date) as varchar) as release_year,
        cast(extract(year from source_release_date) + 1 as varchar)
            as following_year,
        extract(month from source_release_date) as release_month
    from {{ source("world_bank_ids", "raw_ids_metadata") }}
),

extraction as (
    select
        strftime(metadata.downloaded_at at time zone 'UTC', '%Y-%m-%d')
            as last_data_refresh_date,
        case
            when metadata.release_month < 12 then metadata.release_year || '-12'
            else metadata.following_year || '-12'
        end as next_source_release_date
    from metadata
),

observation_range as (
    select
        cast(min(observations.date) as varchar) as earliest_observation_date,
        cast(max(observations.date) as varchar) as latest_observation_date
    from {{ ref("int_ids_observations") }} as observations
)

select
    'World Bank International Debt Statistics (IDS)' as name,
    'dc/s/WorldBank' as source,
    'https://datacatalog.worldbank.org/search/dataset/0038015/international-debt-statistics'
        as url,
    bulk_files.source_data_url,
    'https://datacatalog.worldbank.org/public-licenses?fragment=cc' as license,
    'dcid:CreativeCommonsAttributionLicense' as license_type,
    'P1Y' as source_release_frequency,
    extraction.last_data_refresh_date,
    extraction.next_source_release_date,
    extraction.next_source_release_date as next_data_refresh_date,
    observation_range.earliest_observation_date,
    observation_range.latest_observation_date,
    'International Debt Statistics (IDS) is the World Bank''s annual dataset on '
    || 'the external debt of low- and middle-income countries. It reports debt '
    || 'stocks, disbursements, debt service and projected repayments by borrower and '
    || 'creditor type, with public and publicly guaranteed debt broken down by '
    || 'creditor, from 1970 onwards. The data come mainly from countries'' '
    || 'loan-by-loan reporting of public and publicly guaranteed debt to the World '
    || 'Bank''s Debtor Reporting System, which is a condition of borrowing from IBRD '
    || 'and IDA.' as description,
    'International Debt Statistics by the World Bank, '
    || 'https://datacatalog.worldbank.org/search/dataset/0038015/international-debt-statistics'
    || ', licensed under CC BY 4.0. Modified by The ONE Campaign.'
        as license_attribution
from extraction
cross join observation_range
cross join bulk_files
