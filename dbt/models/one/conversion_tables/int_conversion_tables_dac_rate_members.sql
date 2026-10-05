-- Every DAC1 donor's own implied exchange rate, its national-currency ODA over
-- its US-dollar ODA, in units of its currency per US dollar, one row per donor
-- and year.
--
-- A donor's currency is the one the IMF map gives its country, which is
-- present-tense, so one currency applies to every year. A donor with no country,
-- such as EU Institutions, takes the currency its currency_rate_reference row
-- names. A donor with neither, such as DAC Countries, Total, has no rate.
with countries as (
    select
        code as imf_area_code,
        dcid
    from {{ ref("code_mapping") }}
    where property = 'imfAreaCode'
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
),

seeded_currencies as (
    select
        area,
        currency
    from {{ ref("currency_rate_reference") }}
    where
        recipe = 'dcid:ConversionRecipeDAC'
        and role = 'reference'
),

members as (
    select
        dac_1.entity as area,
        dac_1.year,
        coalesce(country_currencies.currency, seeded_currencies.currency)
            as currency,
        dac_1.national_currency / dac_1.current_usd as rate
    from {{ ref("int_conversion_tables_dac_1_series") }} as dac_1
    left join country_currencies
        on dac_1.entity = country_currencies.country
    left join seeded_currencies
        on dac_1.entity = seeded_currencies.area
    where
        dac_1.national_currency is not null
        and dac_1.current_usd <> 0
)

select
    members.area,
    members.currency,
    currency_units.dcid as currency_unit,
    members.year,
    members.rate,
    false as is_estimate
from members
left join currency_units
    on members.currency = currency_units.iso4217_code
where members.currency is not null
