select
    statvar_dcid as variable,
    year as date,
    financing_provider,
    financing_recipient,
    -- oda-data returns millions, exactly as the CRS does, so this scales
    -- amounts the same way dc_dac_crs_observations.sql does.
    cast(value * 1000000 as decimal(18, 2)) as value,
    unit,
    measurement_method
from {{ ref("int_imputed_multilateral_sectors_mapped") }}
