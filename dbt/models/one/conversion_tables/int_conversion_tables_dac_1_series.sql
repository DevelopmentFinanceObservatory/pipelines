-- One row per donor and year, with total net ODA in each price basis. A basis
-- the donor does not report stays null. DAC Countries, Total and donors that
-- report in US dollars outright have no national-currency row.
select
    entity,
    year,
    any_value(value) filter (where amount_type = 'A') as current_usd,
    any_value(value) filter (where amount_type = 'D') as constant_usd,
    any_value(value) filter (where amount_type = 'N') as national_currency
from {{ ref("int_conversion_tables_dac_1_resolved") }}
group by entity, year
