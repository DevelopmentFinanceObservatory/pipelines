-- The commitments are read from a climate-finance package release, which
-- takes them from the OECD's recipient-perspective CRDF files, so the
-- imported release date is that release's. The release is cut by hand, so the
-- node carries no next release date.
with metadata as (
    select strftime(source_release_date, '%Y-%m-%d') as imported_source_release_date
    from {{ source("one_climate_finance", "raw_climate_finance_metadata") }}
),

observation_range as (
    select
        cast(min(observations.date) as varchar) as earliest_observation_date,
        cast(max(observations.date) as varchar) as latest_observation_date
    from {{ ref("dc_oecd_crdf_observations") }} as observations
)

select
    'OECD Climate-related Development Finance' as name,
    'dc/s/OrganisationForEconomicCo-operationAndDevelopmentOecd' as source,
    'https://webfs.oecd.org/climate/RecipientPerspective/' as source_data_url,
    'https://www.oecd.org/en/about/terms-conditions.html' as license,
    'dcid:OECDTermsAndConditions' as license_type,
    metadata.imported_source_release_date,
    observation_range.earliest_observation_date,
    observation_range.latest_observation_date,
    'https://www.oecd.org/en/topics/sub-issues/'
    || 'development-finance-for-climate-and-the-environment.html' as url,
    'Climate-related development finance commitments the OECD publishes in '
    || 'its CRDF dataset, by provider and recipient. Rio-marked projects count '
    || 'at full value, and development banks'' projects at the climate share '
    || 'the banks report.' as description,
    'Source: OECD, Climate-related Development Finance, recipient '
    || 'perspective, https://webfs.oecd.org/climate/RecipientPerspective/. '
    || 'This is an adaptation of an original work by the OECD. The opinions '
    || 'expressed and arguments employed in the adaptation should not be '
    || 'reported as representing the official views of the OECD or of its '
    || 'member countries.' as license_attribution
from metadata
cross join observation_range
