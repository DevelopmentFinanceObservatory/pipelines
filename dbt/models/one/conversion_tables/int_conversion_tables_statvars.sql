-- The two StatVars of each conversion table, an exchange rate and a deflator.
-- The recipe member is the measurement qualifier, the only property that
-- separates the DAC pair from the IMF pair, so each table's import declares its
-- own two nodes. Neither carries constraints or observation properties, so each
-- observation's subject is its observationAbout. The unit is a rate or an index
-- point, so nothing reads a table as money to convert.
with dc_names as (
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
    from (
        values
        (
            'exchange_rate',
            'dcid:exchangeRate',
            'dcid:CurrencyUnit',
            'dcid:CurrencyPerUSDollar',
            'Exchange rate per US dollar'
        ),
        (
            'deflator',
            'dcid:deflator',
            'dcid:EconomicProduction',
            'dcid:IndexPoint',
            'US-dollar deflator'
        )
    ) as series (series, measured_property, population_type, unit, label)
    cross join (
        values
        ('dcid:ConversionRecipeDAC', 'DAC'),
        ('dcid:ConversionRecipeIMF', 'IMF')
    ) as recipes (recipe, table_label)
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
