{{ config(meta={"dagster": {"ref": {"name": "dc_dac1_flow_observations"}}}) }}
{#-
    Each year's published total for each unit matches the source's to within
    what rounding each observation can lose: half a cent for an amount, half a
    person for population and half a millionth for a percentage. A year or unit
    present on only one side fails too.
-#}
with source_totals as (
    select
        unified.year,
        indicator_mapping.unit,
        sum(
            case
                when indicator_mapping.unit = 'dcid:Percent' then unified.value
                else unified.value * 1000000
            end
        ) as total
    from {{ ref("int_dac1_unified") }} as unified
    inner join {{ ref("dac1_indicator_mapping") }} as indicator_mapping
        on
            unified.aid_type = indicator_mapping.aid_type
            and unified.flow = indicator_mapping.flow
    group by unified.year, indicator_mapping.unit
),

observations as (
    select
        date,
        unit,
        value
    from {{ ref("dc_dac1_flow_observations") }}
    union all
    select
        date,
        unit,
        value
    from {{ ref("dc_dac1_donor_observations") }}
),

published_totals as (
    select
        date as year,
        unit,
        sum(value) as total,
        count(*) as observation_count
    from observations
    group by date, unit
)

select
    source_totals.total as source_total,
    published_totals.total as published_total,
    coalesce(source_totals.year, published_totals.year) as year,
    coalesce(source_totals.unit, published_totals.unit) as unit
from source_totals
full outer join published_totals
    on
        source_totals.year = published_totals.year
        and source_totals.unit is not distinct from published_totals.unit
where
    source_totals.total is null
    or published_totals.total is null
    or abs(source_totals.total - published_totals.total)
    > published_totals.observation_count
    * case
        when published_totals.unit = 'dcid:Percent' then 0.0000005
        when published_totals.unit is null then 0.5
        else 0.005
    end
