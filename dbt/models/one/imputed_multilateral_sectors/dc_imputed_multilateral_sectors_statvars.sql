select distinct
    statvar_dcid as dcid,
    statvar_name as name,
    population_type,
    measured_property,
    stat_type,
    constraint_properties,
    netting_basis,
    financial_flow_type,
    type_of_flow,
    aid_type,
    purpose_code,
    measurement_qualifier,
    'financingProvider,financingRecipient' as observation_properties
from {{ ref("int_imputed_multilateral_sectors_mapped") }}
