-- Each domestic-currency indicator paired with the US-dollar indicator that
-- measures the same thing.
select
    domestic.concept_code as domestic_code,
    dollar.concept_code as dollar_code
from {{ ref("weo_indicator_mapping") }} as domestic
inner join {{ ref("weo_indicator_mapping") }} as dollar
    on
        dollar.unit = 'dcid:CurrencyUSD'
        and domestic.population_type = dollar.population_type
        and domestic.measured_property = dollar.measured_property
        and domestic.stat_type = dollar.stat_type
        and domestic.measurement_qualifier = dollar.measurement_qualifier
        and domestic.product_tax_basis is not distinct from dollar.product_tax_basis
        and domestic.fixed_capital_consumption_basis
        = dollar.fixed_capital_consumption_basis
        and domestic.cyclical_adjustment is not distinct from dollar.cyclical_adjustment
        and domestic.currency_conversion_basis
        is not distinct from dollar.currency_conversion_basis
        and domestic.final_expenditure_type
        is not distinct from dollar.final_expenditure_type
        and domestic.denominator_concept_code
        is not distinct from dollar.denominator_concept_code
        and domestic.denominator_dcid is not distinct from dollar.denominator_dcid
where domestic.unit is null
