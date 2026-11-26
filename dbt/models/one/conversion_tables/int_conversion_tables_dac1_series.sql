-- One row per donor and year, with total net ODA in each price basis. A basis
-- the donor does not report stays null. DAC Countries, Total and donors that
-- report in US dollars outright have no national-currency row.
select
    entity,
    year,
    any_value(case when amount_type = 'A' then value end) as current_usd,
    any_value(case when amount_type = 'D' then value end) as constant_usd,
    any_value(case when amount_type = 'N' then value end) as national_currency
from {{ ref("int_conversion_tables_dac1_resolved") }}
group by entity, year
