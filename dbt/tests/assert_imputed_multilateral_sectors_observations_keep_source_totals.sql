{#-
    Each year's published total matches the source's to within half a cent per
    observation, the most that rounding each observation to cents can lose.
    Rounding before the sum loses up to $5 per source row instead, tens of
    thousands of dollars a year, which this fails on.

    A year present on only one side fails too.
-#}
with source_totals as (
    select
        cast(year as integer) as year,
        sum(cast(value as decimal(38, 12))) * 1000000 as total
    from {{ source("one_imputed_multilateral_sectors", "raw_imputed_multilateral_sectors") }}
    group by year
),

published_totals as (
    select
        date as year,
        sum(value) as total,
        count(*) as observation_count
    from {{ ref("dc_imputed_multilateral_sectors_observations") }}
    group by date
)

select
    source_totals.total as source_total,
    published_totals.total as published_total,
    coalesce(source_totals.year, published_totals.year) as year
from source_totals
full outer join published_totals
    on source_totals.year = published_totals.year
where
    source_totals.total is null
    or published_totals.total is null
    or abs(source_totals.total - published_totals.total)
    > published_totals.observation_count * 0.005
