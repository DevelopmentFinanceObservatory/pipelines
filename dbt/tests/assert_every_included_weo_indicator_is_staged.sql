{{ config(meta={"dagster": {"ref": {"name": "stg_weo"}}}) }}
{# Catches a listed indicator the IMF renames or drops, which the stg_weo filter would otherwise lose silently. #}
select included.concept_code
from {{ ref("weo_staged_indicators") }} as included
where included.concept_code not in (
    select staged.concept_code
    from {{ ref("stg_weo") }} as staged
)
