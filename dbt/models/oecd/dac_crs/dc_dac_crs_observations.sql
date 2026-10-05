select
    statvar_dcid as variable,
    year as date,
    financing_provider,
    financing_recipient,
    -- The CRS reports amounts in millions, so they are scaled up to be loaded into the graph
    cast(value * 1000000 as decimal(18, 2)) as value,
    unit
from {{ ref("int_dac_crs_mapped") }}
