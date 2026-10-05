-- The PPP series carry no last actual year. They are converted from the
-- national accounts behind GDP in US dollars, so they take its year.
with gdp_last_actual_years as (
    select distinct
        observation_about,
        last_actual_year
    from {{ ref("int_weo_resolved") }}
    where concept_code = 'NGDPD'
)

select
    weo.observation_about,
    weo.concept_code,
    weo.time_period,
    weo.unit_code,
    weo.obs_value * weo.scale_code as value,
    weo.time_period > coalesce(
        weo.last_actual_year, gdp_last_actual_years.last_actual_year
    ) as is_estimate
from {{ ref("int_weo_resolved") }} as weo
left join gdp_last_actual_years
    on
        weo.concept_code in ('PPPGDP', 'PPPPC')
        and weo.observation_about = gdp_last_actual_years.observation_about
