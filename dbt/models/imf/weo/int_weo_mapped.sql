{% set constraints = {
    "productTaxBasis": "indicators.product_tax_basis",
    "fixedCapitalConsumptionBasis": "indicators.fixed_capital_consumption_basis",
    "cyclicalAdjustment": "indicators.cyclical_adjustment",
    "currencyConversionBasis": "indicators.currency_conversion_basis",
    "finalExpenditureType": "indicators.final_expenditure_type",
} %}

with indicators as (
    select
        indicator_mapping.*,
        stat_types.dcid_segment as stat_type_dcid_segment
    from {{ ref("weo_indicator_mapping") }} as indicator_mapping
    left join {{ ref("stat_type_mapping") }} as stat_types
        on indicator_mapping.stat_type = stat_types.stat_type
),

-- A share names another indicator's StatVar, minted without a denominator as
-- none is itself a share.
denominators as (
    select
        indicators.concept_code,
        {{
            mint_statvar_dcid(
                stat_type_dcid_segment="indicators.stat_type_dcid_segment",
                measured_property="indicators.measured_property",
                population_type="indicators.population_type",
                constraints=constraints,
                measurement_qualifier="indicators.measurement_qualifier"
            )
        }} as dcid
    from indicators
),

statvars as (
    select
        indicators.concept_code,
        {{
            mint_statvar_dcid(
                stat_type_dcid_segment="indicators.stat_type_dcid_segment",
                measured_property="indicators.measured_property",
                population_type="indicators.population_type",
                constraints=constraints,
                measurement_qualifier="indicators.measurement_qualifier",
                measurement_denominator="coalesce(indicators.denominator_dcid, denominators.dcid)"
            )
        }} as statvar_dcid,
        indicators.statvar_name,
        indicators.population_type,
        indicators.measured_property,
        indicators.stat_type,
        indicators.measurement_qualifier,
        {{ list_constraint_properties(constraints) }} as constraint_properties,
        indicators.product_tax_basis,
        indicators.fixed_capital_consumption_basis,
        indicators.cyclical_adjustment,
        indicators.currency_conversion_basis,
        indicators.final_expenditure_type,
        coalesce(
            indicators.denominator_dcid, denominators.dcid
        ) as measurement_denominator,
        indicators.unit,
        indicators.measurement_method
    from indicators
    left join denominators
        on indicators.denominator_concept_code = denominators.concept_code
),

currencies as (
    select
        areas.dcid as observation_about,
        units.dcid as unit
    from {{ ref("code_mapping") }} as areas
    inner join {{ ref("weo_currency_mapping") }} as area_currencies
        on areas.code = area_currencies.imf_area_code
    inner join {{ ref("code_mapping") }} as units
        on
            area_currencies.iso4217_code = units.code
            and units.property = 'iso4217AlphaCode'
    where areas.property = 'imfEconomyCode'
)

select
    statvars.statvar_dcid,
    statvars.statvar_name,
    weo.concept_code,
    statvars.population_type,
    statvars.measured_property,
    statvars.stat_type,
    statvars.measurement_qualifier,
    statvars.constraint_properties,
    statvars.product_tax_basis,
    statvars.fixed_capital_consumption_basis,
    statvars.cyclical_adjustment,
    statvars.currency_conversion_basis,
    statvars.final_expenditure_type,
    statvars.measurement_denominator,
    weo.observation_about,
    weo.time_period,
    weo.value,
    coalesce(statvars.unit, currencies.unit) as unit,
    coalesce(
        statvars.measurement_method,
        case when weo.is_estimate then 'dcid:Estimated' end
    ) as measurement_method
from {{ ref("int_weo_unified") }} as weo
left join statvars
    on weo.concept_code = statvars.concept_code
left join currencies
    on
        statvars.unit is null
        and weo.observation_about = currencies.observation_about
-- The import publishes only the indicators weo_indicator_mapping lists, whatever
-- else int_weo_unified holds. A dollar country's domestic-currency series copies
-- its US-dollar twin, so only the twin is kept.
where
    weo.concept_code in (
        select indicators.concept_code
        from {{ ref("weo_indicator_mapping") }} as indicators
    )
    and (
        weo.concept_code not in (
            select twins.domestic_code
            from {{ ref("int_weo_dollar_twins") }} as twins
        )
        or weo.observation_about not in (
            select dollar_countries.dcid
            from {{ ref("int_weo_dollar_countries") }} as dollar_countries
        )
    )
