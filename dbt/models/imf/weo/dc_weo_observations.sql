select
    statvar_dcid as variable,
    time_period as date,
    observation_about,
    cast(value as decimal(38, 6)) as value,
    unit,
    measurement_method
from {{ ref("int_weo_mapped") }}
