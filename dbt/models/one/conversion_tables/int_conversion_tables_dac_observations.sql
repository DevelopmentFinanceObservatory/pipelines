-- Every observation of the DAC table, one row per StatVar, entity and year.
--
-- The rates fill per currency. A currency DAC1 reports takes DAC1's rate, and
-- every other currency takes the IMF table's, so aid to Kenya converts to Kenyan
-- shillings. A currency DAC1 reports has no rate in the years DAC1 lacks, such
-- as years after DAC1's latest and years before its donor reported, so no
-- currency's series splices the two sources. From 1999 each donor's own currency
-- reproduces the OECD's national-currency amounts. Before 1999 the euro rate is
-- EU Institutions' ECU rate, while donors' DAC1 euro amounts are legacy
-- currencies converted at the fixed rates. The deflators come from DAC1 alone.
--
-- A rate filled from an IMF projection carries IMFProjection as its measurement
-- method. A reported row carries none. The StatVar join is a left join, so a
-- series with no StatVar leaves a null the not_null test on variable catches.
with imf_fill_rates as (
    select
        imf.currency_unit,
        imf.year,
        imf.rate,
        imf.is_estimate
    from {{ ref("int_conversion_tables_imf_rates") }} as imf
    where imf.currency not in (
        select dac.currency
        from {{ ref("int_conversion_tables_dac_rates") }} as dac
    )
),

table_values as (
    select
        'exchange_rate' as series,
        currency_unit as observation_about,
        year,
        rate as value,
        is_estimate
    from {{ ref("int_conversion_tables_dac_rates") }}

    union all

    select
        'exchange_rate' as series,
        currency_unit as observation_about,
        year,
        rate as value,
        is_estimate
    from imf_fill_rates

    union all

    select
        'deflator' as series,
        entity as observation_about,
        year,
        deflator as value,
        is_estimate
    from {{ ref("int_conversion_tables_dac_deflators") }}
),

statvars as (
    select
        series,
        statvar_dcid,
        unit
    from {{ ref("int_conversion_tables_statvars") }}
    where recipe = 'dcid:ConversionRecipeDAC'
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
