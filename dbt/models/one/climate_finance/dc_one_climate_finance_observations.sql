-- Rounded to cents here, once, after every sum.
select
    statvar_dcid as variable,
    year as date,
    financing_provider,
    financing_recipient,
    cast(value as decimal(18, 2)) as value,
    unit,
    measurement_method
from {{ ref("int_climate_finance_mapped") }}
where import_name = 'one_climate_finance'
