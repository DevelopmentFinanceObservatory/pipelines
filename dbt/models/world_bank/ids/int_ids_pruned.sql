{#-
    The World Bank fills a value for every debtor, creditor, series and year, so most
    of IDS is zero. A zero is kept where it records lending: on a world total, or
    within or after a creditor's lending. It is omitted on a creditor series that is
    zero in every year, before a creditor's first nonzero year, and on an average
    (grace period, grant element, interest rate, maturity) for a year with no
    commitments, where there is nothing to average.
-#}
with series_state as (
    select
        country_code,
        counterpart_code,
        series_code,
        min(year) filter (where value <> 0) as first_nonzero_year
    from {{ ref("stg_ids") }}
    group by country_code, counterpart_code, series_code
),

-- Each average is taken over the commitments to the same group of creditors,
-- named by its third segment: DT.INR.OFFT against DT.COM.OFFT.CD.
commitments as (
    select
        country_code,
        counterpart_code,
        year,
        value,
        split_part(series_code, '.', 3) as creditor_group
    from {{ ref("stg_ids") }}
    where series_code in ('DT.COM.DPPG.CD', 'DT.COM.OFFT.CD', 'DT.COM.PRVT.CD')
),

-- Joined over the zero averages alone, a few million rows rather than every row.
averages_without_commitments as (
    select
        averages.country_code,
        averages.counterpart_code,
        averages.series_code,
        averages.year
    from {{ ref("stg_ids") }} as averages
    left join commitments
        on
            averages.country_code = commitments.country_code
            and averages.counterpart_code = commitments.counterpart_code
            and averages.year = commitments.year
            and split_part(averages.series_code, '.', 3) = commitments.creditor_group
    where
        split_part(averages.series_code, '.', 2) in ('GPA', 'GRE', 'INR', 'MAT')
        and averages.value = 0
        and coalesce(commitments.value, 0) = 0
)

select staged.*
from {{ ref("stg_ids") }} as staged
inner join series_state
    on
        staged.country_code = series_state.country_code
        and staged.counterpart_code = series_state.counterpart_code
        and staged.series_code = series_state.series_code
left join averages_without_commitments
    on
        staged.country_code = averages_without_commitments.country_code
        and staged.counterpart_code = averages_without_commitments.counterpart_code
        and staged.series_code = averages_without_commitments.series_code
        and staged.year = averages_without_commitments.year
where
    staged.value <> 0
    or (
        averages_without_commitments.series_code is null
        and (
            staged.counterpart_code = 'WLD'
            -- Null when the series is zero in every year, so those rows drop too.
            or staged.year > series_state.first_nonzero_year
        )
    )
