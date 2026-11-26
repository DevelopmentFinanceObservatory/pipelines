-- 'counterpart_code' is deliberately not carried forward: it exists in the
-- resolved layer only so that a null creditor can be told apart from a code that
-- failed to resolve, and that check is made there. Here 'creditor is null' marks
-- the WLD all-creditors total.
select
    debtor,
    creditor,
    series_code,
    series_name,
    year,
    value,
    -- split_part returns NULL for an out-of-range position on both adapters, so
    -- the codes carrying three or four segments leave the later positions null.
    {{ dbt.split_part("series_code", "'.'", 1) }} as series_code_pos_1,
    {{ dbt.split_part("series_code", "'.'", 2) }} as series_code_pos_2,
    {{ dbt.split_part("series_code", "'.'", 3) }} as series_code_pos_3,
    {{ dbt.split_part("series_code", "'.'", 4) }} as series_code_pos_4,
    {{ dbt.split_part("series_code", "'.'", 5) }} as series_code_pos_5
from {{ ref("int_ids_resolved") }}
