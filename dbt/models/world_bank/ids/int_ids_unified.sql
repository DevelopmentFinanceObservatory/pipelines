-- 'counterpart_code' is deliberately not carried forward: it exists in the
-- resolved layer only so that a null creditor can be told apart from a code that
-- failed to resolve, and that check is made there. Here 'creditor is null' marks
-- the WLD all-creditors total.
with with_segments as (
    select
        debtor,
        creditor,
        series_code,
        series_name,
        year,
        value,
        str_split(series_code, '.') as segments
    from {{ ref("int_ids_resolved") }}
)

select
    debtor,
    creditor,
    series_code,
    series_name,
    year,
    value,
    -- DuckDB returns NULL for an out-of-range list index, so the codes carrying
    -- three or four segments leave the later positions null.
    segments[1] as series_code_pos_1,
    segments[2] as series_code_pos_2,
    segments[3] as series_code_pos_3,
    segments[4] as series_code_pos_4,
    segments[5] as series_code_pos_5
from with_segments
