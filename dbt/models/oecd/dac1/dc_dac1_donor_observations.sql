select
    statvar_dcid as variable,
    year as date,
    donor as observation_about,
    value,
    unit
from {{ ref("int_dac1_mapped") }}
where entity_property = 'observationAbout'
