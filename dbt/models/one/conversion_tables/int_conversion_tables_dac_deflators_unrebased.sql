-- Each DAC1 donor's US-dollar deflator, and DAC Countries, Total's, before
-- rebasing: 100 x current-dollar ODA / constant-dollar ODA, the deflator the DAC
-- itself applies. A US-dollar deflator moves with both domestic prices and the
-- exchange rate, so converting a current-dollar value at it gives constant
-- dollars of the base year.
select
    entity,
    year,
    false as is_estimate,
    100 * current_usd / constant_usd as deflator
from {{ ref("int_conversion_tables_dac1_series") }}
where
    current_usd is not null
    and constant_usd <> 0
