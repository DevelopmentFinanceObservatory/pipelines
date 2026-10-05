-- Each WEO country's US-dollar deflator before rebasing: the GDP deflator x
-- US-dollar GDP / national-currency GDP, which turns WEO's national-currency
-- deflator into US-dollar terms. A US-dollar deflator moves with both domestic
-- prices and the exchange rate, so converting a current-dollar value at it gives
-- constant dollars of the base year.
select
    country as entity,
    year,
    deflator_is_estimate as is_estimate,
    gdp_deflator * gdp_usd / gdp_national_currency as deflator
from {{ ref("int_conversion_tables_weo_series") }}
where
    gdp_deflator is not null
    and gdp_usd is not null
    and gdp_national_currency <> 0
