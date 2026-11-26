{{ config(meta={"dagster": {"ref": {"name": "int_weo_mapped"}}}) }}
{#
    Catches a country whose domestic currency changed in a new release, which
    weo_currency_mapping would otherwise keep labelling with the old one.
    Letters only, since the release garbles accented names:
    https://github.com/ONEcampaign/imf-reader/issues/45
#}
with release_currencies as (
    select distinct
        ref_area_code as imf_area_code,
        primary_domestic_currency
    from {{ source("imf_weo", "raw_weo") }}
    where primary_domestic_currency is not null
)

select
    release_currencies.imf_area_code,
    release_currencies.primary_domestic_currency as release_currency,
    mapped.primary_domestic_currency as mapped_currency
from release_currencies
inner join {{ ref("weo_currency_mapping") }} as mapped
    on release_currencies.imf_area_code = mapped.imf_area_code
where
    array_to_string(
        regexp_extract_all(lower(release_currencies.primary_domestic_currency), '[a-z]+'), ''
    )
    != array_to_string(
        regexp_extract_all(lower(mapped.primary_domestic_currency), '[a-z]+'), ''
    )
