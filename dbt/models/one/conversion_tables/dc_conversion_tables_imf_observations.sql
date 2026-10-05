select
    variable,
    date,
    observation_about,
    value,
    unit,
    measurement_method
from {{ ref("int_conversion_tables_imf_observations") }}
