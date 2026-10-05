with areas as (
    select
        code as imf_area_code,
        dcid
    from {{ ref("code_mapping") }}
    where property = 'imfAreaCode'
)

select
    weo.concept_code,
    areas.dcid as observation_about,
    weo.time_period,
    weo.obs_value,
    weo.scale_code,
    weo.unit_code,
    weo.last_actual_year
from {{ ref("stg_weo") }} as weo
left join areas
    on weo.ref_area_code = areas.imf_area_code
where weo.ref_area_code not in {{ weo_excluded_areas() }}
