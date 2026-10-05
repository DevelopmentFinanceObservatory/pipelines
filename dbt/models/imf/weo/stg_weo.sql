select
    cast(concept_code as string) as concept_code,
    cast(concept_label as string) as concept_label,
    cast(ref_area_code as string) as ref_area_code,
    cast(time_period as integer) as time_period,
    cast(obs_value as double) as obs_value,
    cast(scale_code as bigint) as scale_code,
    cast(unit_code as string) as unit_code,
    cast(unit_label as string) as unit_label,
    cast(lastactualdate as integer) as last_actual_year
from {{ source("imf_weo", "raw_weo") }}
where
    concept_code in (
        select indicators.concept_code
        from {{ ref("weo_staged_indicators") }} as indicators
    )
