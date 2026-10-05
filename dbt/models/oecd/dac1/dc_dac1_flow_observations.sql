select
    statvar_dcid as variable,
    year as date,
    donor as financing_provider,
    recipient as financing_recipient,
    unit,
    value
from {{ ref("int_dac1_mapped") }}
where entity_property = 'financingProvider'
