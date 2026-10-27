-- The dataset is computed from two OECD dataflows, CRS and Members' Total Use
-- of the Multilateral System, so the source data is two URLs. sourceDataUrl
-- holds one string, so they are comma-delimited.
-- FIXME: Emit the two URLs as separate values once dcp-tools accepts a list:
-- https://github.com/ONEcampaign/dcp-tools/issues/176
with dataflows as (
    select array_to_string(
        [
            'https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/DSD_CRS@DF_CRS/',
            'https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/DSD_MULTI@DF_MULTI/'
        ],
        ','
    ) as source_data_url
),

metadata as (
    select
        downloaded_at,
        crs_release_date,
        multisystem_release_date,
        extract(year from crs_release_date) as crs_release_year,
        extract(year from multisystem_release_date) as multisystem_release_year
    from {{
        source(
            "one_imputed_multilateral_sectors",
            "raw_imputed_multilateral_sectors_metadata"
        )
    }}
),

extraction as (
    select
        cast(metadata.crs_release_year as varchar) as crs_release_year,
        cast(metadata.multisystem_release_year as varchar)
            as multisystem_release_year,
        cast(
            greatest(metadata.crs_release_year, metadata.multisystem_release_year)
            as varchar
        ) as latest_release_year,
        strftime(
            greatest(metadata.crs_release_date, metadata.multisystem_release_date),
            '%Y-%m-%d'
        ) as imported_source_release_date,
        strftime(metadata.downloaded_at at time zone 'UTC', '%Y-%m-%d')
            as access_date,
        -- CRS and Members' Total Use each have their own publication cycle
        -- (next_oecd_release_date, next_multisystem_release_date); the source
        -- next in line is whichever of the two comes first.
        least(
            {{ next_oecd_release_date('metadata.crs_release_date') }},
            {{ next_multisystem_release_date('metadata.multisystem_release_date') }}
        ) as next_source_release_date
    from metadata
),

observation_range as (
    select
        cast(min(observations.date) as varchar) as earliest_observation_date,
        cast(max(observations.date) as varchar) as latest_observation_date
    from {{ ref("dc_imputed_multilateral_sectors_observations") }} as observations
)

select
    'ONE Imputed Multilateral Sectors' as name,
    'source/ONEData' as source,
    'https://docs.one.org/methodologies/data/oda/imputed-multilateral-sectors/'
        as url,
    dataflows.source_data_url,
    'https://www.oecd.org/en/about/terms-conditions.html' as license,
    'dcid:OECDTermsAndConditions' as license_type,
    'P3M' as source_release_frequency,
    extraction.imported_source_release_date,
    extraction.next_source_release_date,
    extraction.next_source_release_date as next_data_refresh_date,
    observation_range.earliest_observation_date,
    observation_range.latest_observation_date,
    'Imputed Multilateral Sectors is ONE''s estimate of how each DAC '
    || 'donor''s core contributions to multilateral organisations are '
    || 'spent, by recipient and sector. ONE computes it from the OECD '
    || 'Creditor Reporting System and Members'' Total Use of the '
    || 'Multilateral System, attributing each multilateral organisation''s '
    || 'spending to its donors in proportion to their core contributions '
    || 'to that organisation. Values are gross ODA disbursements in '
    || 'current US dollars.' as description,
    'ONE (' || extraction.latest_release_year || '), Imputed Multilateral Sectors, '
    || 'https://docs.one.org/methodologies/data/oda/imputed-multilateral-sectors/ '
    || '(accessed on ' || extraction.access_date || '), computed from '
    || 'OECD (' || extraction.crs_release_year || '), Creditor Reporting '
    || 'System (CRS), '
    || 'https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/DSD_CRS@DF_CRS/ '
    || '(accessed on ' || extraction.access_date || ') and OECD ('
    || extraction.multisystem_release_year || '), Members'' Total Use of the '
    || 'Multilateral System, '
    || 'https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/DSD_MULTI@DF_MULTI/ '
    || '(accessed on ' || extraction.access_date || ')'
        as license_attribution
from extraction
cross join observation_range
cross join dataflows
