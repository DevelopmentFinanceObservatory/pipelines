-- noqa: disable=RF05
{#
    The unpivot shouldn't lose or duplicate cells, so the test here counts the year
    columns as every column but the six dimensions, a different mechanism from
    stg_ids' four-digit column names. The series stg_ids drops as out of scope are
    left out of the source count too.

    UNPIVOT drops NULLs by default on both sides, so both counts are of non-null cells.
#}
{%- set dimensions = [
    "Country Code",
    "Country Name",
    "Counterpart-Area Name",
    "Counterpart-Area Code",
    "Series Name",
    "Series Code",
] -%}
{%- set year_columns = [] -%}
{%- for column in adapter.get_columns_in_relation(source("world_bank_ids", "raw_ids")) -%}
    {%- if column.name not in dimensions -%}
        {%- do year_columns.append(adapter.quote(column.name)) -%}
    {%- endif -%}
{%- endfor %}

with source_cells as (
    select count(*) as cell_count
    from {{ source("world_bank_ids", "raw_ids") }}
    unpivot (value for year in ({{ year_columns | join(", ") }}))
    where trim({{ adapter.quote("Series Code") }}) not in {{ ids_excluded_series() }}
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
