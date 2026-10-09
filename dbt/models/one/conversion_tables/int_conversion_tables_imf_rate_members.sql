-- Every WEO country's own implied exchange rate, its national-currency GDP over
-- its US-dollar GDP, in units of its currency per US dollar, one row per country
-- and year.
--
-- A country's currency is the one the IMF map gives it, which is present-tense,
-- so one currency applies to every year. A country the map leaves out keeps a
-- null currency, which the not_null test on currency catches.
with countries as (
    select
        code as imf_area_code,
        dcid
    from {{ ref("code_mapping") }}
    where property = 'imfEconomyCode'
),

currency_units as (
    select
        code as iso4217_code,
        dcid
    from {{ ref("code_mapping") }}
    where property = 'iso4217AlphaCode'
),

country_currencies as (
    select
        countries.dcid as country,
        currency_map.iso4217_code as currency
    from {{ ref("weo_currency_mapping") }} as currency_map
    left join countries
        on currency_map.imf_area_code = countries.imf_area_code
)

select
    weo.country as area,
    country_currencies.currency,
    currency_units.dcid as currency_unit,
    weo.year,
    weo.rate_is_estimate as is_estimate,
    weo.gdp_national_currency / weo.gdp_usd as rate
from {{ ref("int_conversion_tables_weo_series") }} as weo
left join country_currencies
    on weo.country = country_currencies.country
left join currency_units
    on country_currencies.currency = currency_units.iso4217_code
where
    weo.gdp_national_currency is not null
    and weo.gdp_usd <> 0
