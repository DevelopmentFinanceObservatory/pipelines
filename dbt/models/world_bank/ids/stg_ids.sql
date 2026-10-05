with unpivoted as (
    select
        year,
        value,
        trim("Country Code") as country_code,
        trim("Country Name") as country_name,
        trim("Counterpart-Area Name") as counterpart_name,
        trim("Counterpart-Area Code") as counterpart_code,
        trim("Series Name") as series_name,
        trim("Series Code") as series_code
    from (  -- unpivot drops rows with NULL value automatically
        unpivot {{ source("world_bank_ids", "raw_ids") }}
        on columns('^[0-9]{4}$')
        into name year value value
    )
)

select
    country_code,
    country_name,
    counterpart_name,
    counterpart_code,
    series_name,
    series_code,
    cast(year as integer) as year,
    -- decimal(38, 5) used here as decimal(18, 5) (as per CRS) not sufficient
    -- to represent magnitude of the raw data
    cast(value as decimal(38, 5)) as value
from unpivoted
where series_code not in {{ ids_excluded_series() }}
