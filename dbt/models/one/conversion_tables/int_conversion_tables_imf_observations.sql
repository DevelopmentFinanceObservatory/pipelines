-- Every observation of the IMF table, one row per StatVar, entity and year. Its
-- rates and deflators both come from WEO.
--
-- A row computed from an IMF projection carries IMFProjection as its
-- measurement method. A reported row carries none. The StatVar join is a left
-- join, so a series with no StatVar leaves a null the not_null test on variable
-- catches.
with table_values as (
    select
        'exchange_rate' as series,
        currency_unit as observation_about,
        year,
        rate as value,
        is_estimate
    from {{ ref("int_conversion_tables_imf_rates") }}

    union all

    select
        'deflator' as series,
        entity as observation_about,
        year,
        deflator as value,
        is_estimate
    from {{ ref("int_conversion_tables_imf_deflators") }}
),

statvars as (
    select
        series,
        statvar_dcid,
        unit
    from {{ ref("int_conversion_tables_statvars") }}
    where recipe = 'dcid:ConversionRecipeIMF'
)

select
    statvars.statvar_dcid as variable,
    table_values.year as date,
    table_values.observation_about,
    table_values.value,
    statvars.unit,
    case when table_values.is_estimate then 'dcid:IMFProjection' end
        as measurement_method
from table_values
left join statvars
    on table_values.series = statvars.series
