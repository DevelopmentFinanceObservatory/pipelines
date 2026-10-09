-- The PPP series carry no last actual year. They are converted from the
-- national accounts behind GDP in US dollars, so they take its year.
with gdp_last_actual_years as (
    select distinct
        observation_about,
        last_actual_year
    from {{ ref("int_weo_resolved") }}
    where concept_code = 'NGDPD'
),

-- Country groups other than the euro area carry no last actual year either.
-- WEO projects from the year it is released, so they take the year before.
release_last_actual_year as (
    select extract(year from source_release_date) - 1 as last_actual_year
    from {{ source("imf_weo", "raw_weo_metadata") }}
)

select
    weo.observation_about,
    weo.concept_code,
    weo.time_period,
    weo.unit_code,
    weo.obs_value * weo.scale_code as value,
    weo.time_period > coalesce(
        weo.last_actual_year,
        gdp_last_actual_years.last_actual_year,
        release_last_actual_year.last_actual_year
    ) as is_estimate
from {{ ref("int_weo_resolved") }} as weo
left join gdp_last_actual_years
    on
        weo.concept_code in ('PPPGDP', 'PPPPC')
        and weo.observation_about = gdp_last_actual_years.observation_about
cross join release_last_actual_year
