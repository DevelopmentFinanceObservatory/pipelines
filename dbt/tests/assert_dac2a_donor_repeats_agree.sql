{{ config(meta={"dagster": {"ref": {"name": "int_dac2a_resolved"}}}) }}
{# Fails if two donor codes sharing a node disagree, as int_dac2a_unified would add them. #}
select
    recipient_code,
    donor,
    aid_type,
    year
from {{ ref("int_dac2a_resolved") }}
where amount_type = 'A'
group by recipient_code, donor, aid_type, year
having count(distinct value) > 1
