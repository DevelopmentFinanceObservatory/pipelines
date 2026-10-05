select
    statvar_dcid as dcid,
    statvar_name as name,
    population_type,
    measured_property,
    stat_type,
    measurement_qualifier
from {{ ref("int_conversion_tables_statvars") }}
where recipe = 'dcid:ConversionRecipeDAC'
