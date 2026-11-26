with unpivoted as (
    select
        year,
        value,
        trim({{ adapter.quote("Country Code") }}) as country_code,
        trim({{ adapter.quote("Country Name") }}) as country_name,
        trim({{ adapter.quote("Counterpart-Area Name") }}) as counterpart_name,
        -- Counterpart codes are three-digit strings ("004"), but the bulk XLSX
        -- files store some cells as numbers, which arrive as "4". Restore the
        -- zero padding so both forms resolve to the same creditor. Letter codes
        -- such as WLD pass through.
        lpad(
            trim({{ adapter.quote("Counterpart-Area Code") }}),
            cast(
                greatest(length(trim({{ adapter.quote("Counterpart-Area Code") }})), 3)
                as integer
            ),
            '0'
        ) as counterpart_code,
        trim({{ adapter.quote("Series Name") }}) as series_name,
        trim({{ adapter.quote("Series Code") }}) as series_code
    -- unpivot drops rows with a null value
    from {{ source("world_bank_ids", "raw_ids") }}
    unpivot (value for year in (
        {%- for year in ids_year_columns() %}
        {{ adapter.quote(year) }}{{ "," if not loop.last }}
        {%- endfor %}
    ))
)

select
    country_code,
    country_name,
    counterpart_name,
    counterpart_code,
    series_name,
    series_code,
    cast(year as integer) as year,
    cast(value as {{ dbt.type_float() }}) as value
from unpivoted
where series_code not in {{ ids_excluded_series() }}
