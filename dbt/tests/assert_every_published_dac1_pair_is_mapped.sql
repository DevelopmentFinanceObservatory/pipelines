{{ config(meta={"dagster": {"ref": {"name": "int_dac1_unified"}}}) }}
{# Catches an aid type or flow DAC1 starts publishing that dac1_indicator_mapping does not map yet. #}
select distinct
    unified.aid_type,
    unified.flow
from {{ ref("int_dac1_unified") }} as unified
left join {{ ref("dac1_indicator_mapping") }} as mapping
    on
        unified.aid_type = mapping.aid_type
        and unified.flow = mapping.flow
where mapping.aid_type is null
