-- One row per country and year, with the three WEO series the IMF conversion
-- table is computed from. A series WEO does not publish for that year stays null.
-- Each derived value is a projection if any series it is computed from is one, so
-- the rate and the deflator each carry their own flag.
select
    observation_about as country,
    time_period as year,
    any_value(case when concept_code = 'NGDP' then value end) as gdp_national_currency,
    any_value(case when concept_code = 'NGDPD' then value end) as gdp_usd,
    any_value(case when concept_code = 'NGDP_D' then value end) as gdp_deflator,
    coalesce(
        {{ dbt.bool_or("case when concept_code in ('NGDP', 'NGDPD') then is_estimate end") }},
        false
    ) as rate_is_estimate,
    {{ dbt.bool_or("is_estimate") }} as deflator_is_estimate
from {{ ref("int_weo_unified") }}
where concept_code in ('NGDP', 'NGDPD', 'NGDP_D')
group by observation_about, time_period
