-- The two StatVars of each conversion table, an exchange rate and a deflator.
-- The recipe member is the measurement qualifier, the only property that
-- separates the DAC pair from the IMF pair, so each table's import declares its
-- own two nodes. Neither carries constraints or observation properties, so each
-- observation's subject is its observationAbout. The unit is a rate or an index
-- point, so nothing reads a table as money to convert.
with series as (
    select
        'exchange_rate' as series,
        'dcid:exchangeRate' as measured_property,
        'dcid:CurrencyUnit' as population_type,
        'dcid:CurrencyPerUSDollar' as unit,  -- noqa: RF04
        'Exchange rate per US dollar' as label  -- noqa: RF04
    union all
    select
        'deflator' as series,
        'dcid:deflator' as measured_property,
        'dcid:EconomicProduction' as population_type,
        'dcid:IndexPoint' as unit,  -- noqa: RF04
        'US-dollar deflator' as label  -- noqa: RF04
),

recipes as (
    select
        'dcid:ConversionRecipeDAC' as recipe,
        'DAC' as table_label
    union all
    select
        'dcid:ConversionRecipeIMF' as recipe,
        'IMF' as table_label
),

dc_names as (
    select
        recipes.recipe,
        series.series,
        series.measured_property,
        series.population_type,
        series.unit,
        'dcid:measuredValue' as stat_type,
        recipes.recipe as measurement_qualifier,
        series.label || ' (' || recipes.table_label || ' conversion table)'
            as statvar_name
    from series
    cross join recipes
)

select
    {{
        mint_statvar_dcid(
            stat_type_dcid_segment="stat_types.dcid_segment",
            measured_property="dc_names.measured_property",
            population_type="dc_names.population_type",
            constraints={},
            measurement_qualifier="dc_names.measurement_qualifier"
        )
    }} as statvar_dcid,
    dc_names.statvar_name,
    dc_names.recipe,
    dc_names.series,
    dc_names.population_type,
    dc_names.measured_property,
    dc_names.stat_type,
    dc_names.measurement_qualifier,
    dc_names.unit
from dc_names
left join {{ ref("stat_type_mapping") }} as stat_types
    on dc_names.stat_type = stat_types.stat_type
