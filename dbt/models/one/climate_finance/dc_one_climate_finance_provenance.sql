-- The Climate Finance Files methodology page documents the data. The package
-- that computes it is private, so the source data URLs point to the three
-- public inputs: the CRS, the CRDF and the parties' Biennial Transparency
-- Reports, the last for the climate shares that weight donors' Rio-marked
-- projects. The page belongs to the 2026 edition and the methodology has no
-- edition-independent page, so a release that follows a new edition moves
-- this link. sourceDataUrl holds one string, so the URLs are comma-delimited.
-- FIXME: Emit the three URLs as separate values once dcp-tools accepts a list:
-- https://github.com/ONEcampaign/dcp-tools/issues/176
-- ONE Data cuts releases by hand, on no calendar, so the node carries no
-- release frequency and no next release date.
with metadata as (
    select
        cast(extract(year from source_release_date) as string) as release_year,
        cast(source_release_date as string) as imported_source_release_date
    from {{ source("one_climate_finance", "raw_climate_finance_metadata") }}
),

observation_range as (
    select
        cast(min(observations.date) as string) as earliest_observation_date,
        cast(max(observations.date) as string) as latest_observation_date
    from {{ ref("dc_one_climate_finance_observations") }} as observations
)

select
    'ONE Data Climate Finance' as name,
    'source/ONEData' as source,
    'https://www.oecd.org/en/about/terms-conditions.html' as license,
    'dcid:OECDTermsAndConditions' as license_type,
    metadata.imported_source_release_date,
    observation_range.earliest_observation_date,
    observation_range.latest_observation_date,
    'https://docs.one.org/methodologies/deep-dives/climate-finance-files-2026/'
        as url,
    array_to_string(
        [
            'https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS.parquet',
            'https://webfs.oecd.org/climate/RecipientPerspective/',
            'https://unfccc.int/first-biennial-transparency-reports'
        ],
        ','
    ) as source_data_url,
    '{{ dbt.escape_single_quotes(config.meta_require("dataset_description")) }}' as description,
    'ONE Data (' || metadata.release_year || '), ONE Data Climate Finance, '
    || 'computed from OECD, Creditor Reporting System (CRS) and '
    || 'Climate-related Development Finance (CRDF), and UNFCCC, Biennial '
    || 'Transparency Reports. This is an adaptation of '
    || 'an original work by the OECD. The opinions expressed and arguments '
    || 'employed in the adaptation should not be reported as representing the '
    || 'official views of the OECD or of its member countries.'
        as license_attribution
from metadata
cross join observation_range
