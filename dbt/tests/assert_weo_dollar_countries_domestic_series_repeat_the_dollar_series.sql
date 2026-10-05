{#
    A dollar country's domestic-currency observations that int_weo_mapped drops
    must equal their US-dollar twin's. Returns any that don't.
#}
with dollar_country_observations as (
    select * from {{ ref("int_weo_unified") }}
    where observation_about in (
        select dollar_countries.dcid
        from {{ ref("int_weo_dollar_countries") }} as dollar_countries
    )
)

select domestic.*
from {{ ref("int_weo_dollar_twins") }} as twins
inner join dollar_country_observations as domestic
    on twins.domestic_code = domestic.concept_code
left join dollar_country_observations as dollar
    on
        twins.dollar_code = dollar.concept_code
        and domestic.observation_about = dollar.observation_about
        and domestic.time_period = dollar.time_period
where
    dollar.value is null
    or dollar.value != domestic.value
    or dollar.is_estimate != domestic.is_estimate
