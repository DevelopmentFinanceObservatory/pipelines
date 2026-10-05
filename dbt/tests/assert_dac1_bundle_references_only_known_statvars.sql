{{ config(meta={"dagster": {"ref": {"name": "dc_dac1_statvars"}}}) }}
-- Every StatVar the bundle references, as an observation's variable or a share's
-- denominator, is one the import defines or one the mapping names as defined
-- elsewhere, such as GNI and population.
with referenced as (
    select variable as dcid from {{ ref("dc_dac1_flow_observations") }}
    union
    select variable as dcid from {{ ref("dc_dac1_donor_observations") }}
    union
    select measurement_denominator as dcid from {{ ref("dc_dac1_statvars") }}
    where measurement_denominator is not null
),

known as (
    select dcid from {{ ref("dc_dac1_statvars") }}
    union
    select statvar_dcid as dcid from {{ ref("dac1_indicator_mapping") }}
    where statvar_dcid is not null
)

select referenced.dcid
from referenced
where referenced.dcid not in (select known.dcid from known)
