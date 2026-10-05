-- noqa: disable=RF05
{#
    The unpivot shouldn't lose or duplicate cells, so the test here counts the year
    columns by EXCLUDE-ing the six dimensions so as to use a different mechanism from
    stg_ids' COLUMNS('^[0-9]{4}$'). The series stg_ids drops as out of scope are left
    out of the source count too.

    UNPIVOT drops NULLs by default on both sides, so both counts are of non-null cells.
#}
with source_cells as (
    select count(*) as cell_count
    from (
        unpivot {{ source("world_bank_ids", "raw_ids") }}
        on columns(* exclude (
            "Country Code",
            "Country Name",
            "Counterpart-Area Name",
            "Counterpart-Area Code",
            "Series Name",
            "Series Code"
        ))
        into name year value value
    )
    where trim("Series Code") not in {{ ids_excluded_series() }}
),

staged as (
    select count(*) as cell_count
    from {{ ref("stg_ids") }}
)

select
    source_cells.cell_count as source_cell_count,
    staged.cell_count as staged_cell_count
from source_cells
cross join staged
where source_cells.cell_count <> staged.cell_count
