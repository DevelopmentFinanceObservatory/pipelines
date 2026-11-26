select
    statvar_dcid as variable,
    time_period as date,
    observation_about,
    {{ round_significant("value") }} as value,
    unit,
    measurement_method
from {{ ref("int_weo_mapped") }}
