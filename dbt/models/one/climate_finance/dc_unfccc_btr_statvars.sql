select statvars.*
from {{ ref("int_climate_finance_statvars") }} as statvars
where statvars.dcid in (
    select mapped.statvar_dcid
    from {{ ref("int_climate_finance_mapped") }} as mapped
    where mapped.import_name = 'unfccc_btr'
)
