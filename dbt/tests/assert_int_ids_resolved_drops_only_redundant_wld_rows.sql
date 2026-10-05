{#-
    Resolution drops the WLD rows that restate a sibling naming the same lender, and
    nothing else. Counted rather than joined: the models hold 147 million rows, and a
    row-level anti-join proving the same thing would scan both of them twice.

    Three counts, because the three ways this can go wrong are independent:

    - an itemised row is dropped
    - a WLD row on a series that names no lender is dropped
    - a WLD row on a series that does name one is dropped when the source published
      nothing else for that cell, which is the only record of it

    The complementary failure - a redundant WLD row kept rather than dropped - is
    caught by one_row_per_debtor_creditor_series_and_year, because the row and the
    one it restates would name the same lender on the same debtor, series and year.
-#}
with lender_segments as (
    select
        code,
        dcid
    from {{ ref("code_mapping") }}
    where list = 'dcid:Organization' and property = 'wbCode'
),

counterparts as (
    select
        code,
        dcid
    from {{ ref("code_mapping") }}
    where property = 'wbCounterpartCode'
),

-- The cells a lender-coded series reports only against WLD. Their WLD row is the
-- only record of the value, so resolution keeps it and backfills the lender.
unmatched_cells as (
    select
        wld.series_code,
        wld.country_code,
        wld.year
    from {{ ref("int_ids_pruned") }} as wld
    inner join lender_segments
        on split_part(wld.series_code, '.', 3) = lender_segments.code
    where
        wld.counterpart_code = 'WLD'
        and not exists (
            select 1
            from {{ ref("int_ids_pruned") }} as itemised
            inner join counterparts
                on itemised.counterpart_code = counterparts.code
            where
                itemised.series_code = wld.series_code
                and itemised.country_code = wld.country_code
                and itemised.year = wld.year
                and lender_segments.dcid = counterparts.dcid
        )
),

source as (
    select
        count(*) filter (where counterpart_code <> 'WLD') as itemised,
        count(*) filter (
            where
            counterpart_code = 'WLD'
            and split_part(series_code, '.', 3)
            not in (select lender_segments.code from lender_segments)
        ) as totals_on_other_series,
        (select count(*) from unmatched_cells) as totals_with_no_itemised_row
    from {{ ref("int_ids_pruned") }}
),

resolved as (
    select
        count(*) filter (where counterpart_code <> 'WLD') as itemised,
        count(*) filter (
            where
            counterpart_code = 'WLD'
            and split_part(series_code, '.', 3)
            not in (select lender_segments.code from lender_segments)
        ) as totals_on_other_series,
        count(*) filter (
            where
            counterpart_code = 'WLD'
            and split_part(series_code, '.', 3)
            in (select lender_segments.code from lender_segments)
        ) as totals_with_no_itemised_row
    from {{ ref("int_ids_resolved") }}
)

select  -- noqa: ST06
    source.itemised as source_itemised,
    resolved.itemised as resolved_itemised,
    source.totals_on_other_series as source_totals,
    resolved.totals_on_other_series as resolved_totals,
    source.totals_with_no_itemised_row as source_unmatched_totals,
    resolved.totals_with_no_itemised_row as resolved_unmatched_totals
from source
cross join resolved
where
    source.itemised <> resolved.itemised
    or source.totals_on_other_series <> resolved.totals_on_other_series
    or source.totals_with_no_itemised_row <> resolved.totals_with_no_itemised_row
