select
    variable,
    date,
    financing_recipient as observation_about,
    value,
    unit,
    measurement_method
from {{ ref("int_ids_observations") }}
where entity_property = 'observationAbout'
