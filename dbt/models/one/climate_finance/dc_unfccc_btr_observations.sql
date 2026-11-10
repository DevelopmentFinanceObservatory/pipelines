-- Rounded to cents here, once, after every sum. The totals cover every
-- recipient, so the observations name no recipient.
select
    statvar_dcid as variable,
    year as date,
    financing_provider,
    cast(value as decimal(18, 2)) as value,
    unit,
    measurement_method
from {{ ref("int_climate_finance_mapped") }}
where import_name = 'unfccc_btr'
