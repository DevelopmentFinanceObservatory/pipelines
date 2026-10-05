select distinct
    statvar_dcid as dcid,
    statvar_name as name,
    population_type,
    measured_property,
    stat_type,
    constraint_properties,
    type_of_flow,
    type_of_finance,
    cooperation_modality,
    purpose_code,
    financial_obligation_type,
    financial_flow_type,
    netting_basis,
    measurement_qualifier,
    'financingProvider,financingRecipient' as observation_properties
from {{ ref("int_dac_crs_mapped") }}
