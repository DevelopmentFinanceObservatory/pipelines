{{ config(meta={"dagster": {"ref": {"name": "dc_dac2b_observations"}}}) }}
{#-
    Each year's published total for each unit matches the source's to within
    what rounding each observation can lose, half a cent. A year or unit
    present on only one side fails too.
-#}
with source_totals as (
    select
        unified.year,
        indicator_mapping.unit,
        sum(unified.value * 1000000) as total
    from {{ ref("int_dac2b_unified") }} as unified
    inner join {{ ref("dac2b_indicator_mapping") }} as indicator_mapping
        on unified.aid_type = indicator_mapping.aid_type
    group by unified.year, indicator_mapping.unit
),

published_totals as (
    select
        date as year,
        unit,
        sum(value) as total,
        count(*) as observation_count
    from {{ ref("dc_dac2b_observations") }}
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
        and source_totals.unit = published_totals.unit
where
    source_totals.total is null
    or published_totals.total is null
    or abs(source_totals.total - published_totals.total)
    > published_totals.observation_count * 0.005
