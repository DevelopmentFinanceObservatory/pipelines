select
    variable,
    date,
    financing_provider,
    financing_recipient,
    {{ round_significant("value") }} as value,
    unit,
    measurement_method
from {{ ref("int_ids_observations") }}
where entity_property = 'financingRecipient'
