-- The Climate Finance Files methodology page documents the data. The package
-- that computes it is private, so the source data is the three public inputs:
-- the CRS, the CRDF and the parties' Biennial Transparency Reports, the last
-- for the coefficients donors report to the UNFCCC. sourceDataUrl holds one
-- string, so they are comma-delimited.
-- FIXME: Emit the three URLs as separate values once dcp-tools accepts a list:
-- https://github.com/ONEcampaign/dcp-tools/issues/176
-- ONE Data cuts releases by hand, on no calendar, so the node carries no
-- release frequency and no next release date.
with metadata as (
    select
        cast(extract(year from source_release_date) as varchar) as release_year,
        strftime(source_release_date, '%Y-%m-%d') as imported_source_release_date
    from {{ source("one_climate_finance", "raw_climate_finance_metadata") }}
),

observation_range as (
    select
        cast(min(observations.date) as varchar) as earliest_observation_date,
        cast(max(observations.date) as varchar) as latest_observation_date
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
    'Climate finance commitments and payments estimated by ONE Data with its '
    || 'climate-finance package, from the OECD''s CRS and CRDF. Each CRS '
    || 'payment is linked to the commitment it pays and takes that '
    || 'commitment''s climate share. Marker-score weighting uses the '
    || 'coefficients each donor applies when it reports to the UNFCCC, else '
    || 'the shares its Biennial Transparency Report implies, else 40% of a '
    || 'significant project and 100% of a principal one. Under both '
    || 'weightings, development banks'' projects count at the climate share '
    || 'the banks report. Spending-share attribution takes each '
    || 'institution''s climate share from its commitments over the year and '
    || 'the year before, or the OECD''s share where the institution reports no '
    || 'CRS data. Funding-share attribution uses the shares the OECD '
    || 'attributes to developed countries in its count towards the USD 100bn '
    || 'goal.' as description,
    'ONE Data (' || metadata.release_year || '), ONE Data Climate Finance, '
    || 'computed from OECD, Creditor Reporting System (CRS) and '
    || 'Climate-related Development Finance (CRDF). This is an adaptation of '
    || 'an original work by the OECD. The opinions expressed and arguments '
    || 'employed in the adaptation should not be reported as representing the '
    || 'official views of the OECD or of its member countries.'
        as license_attribution
from metadata
cross join observation_range
