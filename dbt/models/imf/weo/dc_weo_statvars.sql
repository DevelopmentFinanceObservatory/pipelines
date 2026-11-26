select
    statvar_dcid as dcid,
    statvar_name as name,
    population_type,
    measured_property,
    stat_type,
    measurement_qualifier,
    measurement_denominator,
    constraint_properties,
    product_tax_basis,
    fixed_capital_consumption_basis,
    cyclical_adjustment,
    currency_conversion_basis,
    final_expenditure_type,
    '"' || string_agg(distinct concept_code, '","' order by concept_code) || '"'
        as source_indicator_code
from {{ ref("int_weo_mapped") }}
group by all

union all by name

select
    *,
    cast(null as string) as source_indicator_code
from {{ ref("weo_extra_statvars") }}
