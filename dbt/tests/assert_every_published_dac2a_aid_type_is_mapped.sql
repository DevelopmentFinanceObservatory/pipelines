{{ config(meta={"dagster": {"ref": {"name": "int_dac2a_unified"}}}) }}
{# Catches an aid type DAC2a starts publishing that dac2a_indicator_mapping does not map yet. #}
select distinct unified.aid_type
from {{ ref("int_dac2a_unified") }} as unified
left join {{ ref("dac2a_indicator_mapping") }} as indicator_mapping
    on unified.aid_type = indicator_mapping.aid_type
where indicator_mapping.aid_type is null
