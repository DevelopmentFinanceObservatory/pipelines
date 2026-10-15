{#-
    Each year's published total matches the source's to within half a cent per
    observation, the most that rounding each observation to cents can lose.
    Rounding before the sum loses up to $5 per source row instead, which this
    fails on.

    The four published measures are commitments, gross disbursements, net
    disbursements and grant equivalents. Net disbursements are disbursements less
    amounts received, so together they total commitments + 2 x disbursements -
    received + grant equivalents. A year present on only one side fails too.
-#}
with source_totals as (
    select
        year,
        (
            coalesce(sum(cast(usd_commitment as decimal(38, 12))), 0)
            + 2 * coalesce(sum(cast(usd_disbursement as decimal(38, 12))), 0)
            - coalesce(sum(cast(usd_received as decimal(38, 12))), 0)
            + coalesce(sum(cast(usd_grant_equiv as decimal(38, 12))), 0)
        ) * 1000000 as total
    from {{ source("oecd_dac_crs", "raw_dac_crs") }}
    where
        bi_multi is distinct from 2
        and {{ exclude_incomplete_years("oecd_dac_crs") }}
    group by year
),

published_totals as (
    select
        date as year,
        sum(value) as total,
        count(*) as observation_count
    from {{ ref("dc_dac_crs_observations") }}
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
