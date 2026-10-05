-- One row per country and year, with the three WEO series the IMF conversion
-- table is computed from. A series WEO does not publish for that year stays null.
-- Each derived value is a projection if any series it is computed from is one, so
-- the rate and the deflator each carry their own flag.
select
    observation_about as country,
    time_period as year,
    any_value(value) filter (where concept_code = 'NGDP') as gdp_national_currency,
    any_value(value) filter (where concept_code = 'NGDPD') as gdp_usd,
    any_value(value) filter (where concept_code = 'NGDP_D') as gdp_deflator,
    coalesce(
        bool_or(is_estimate) filter (where concept_code in ('NGDP', 'NGDPD')),
        false
    ) as rate_is_estimate,
    bool_or(is_estimate) as deflator_is_estimate
from {{ ref("int_weo_unified") }}
where concept_code in ('NGDP', 'NGDPD', 'NGDP_D')
group by observation_about, time_period
