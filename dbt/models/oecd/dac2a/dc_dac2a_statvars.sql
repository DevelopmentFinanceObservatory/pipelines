select distinct
    statvar_dcid as dcid,
    statvar_name as name,
    population_type,
    measured_property,
    stat_type,
    measurement_qualifier,
    measurement_denominator,
    constraint_properties,
    financial_flow_type,
    netting_basis,
    dac_type_of_finance,
    dac2a_aid_type,
    'financingProvider,financingRecipient' as observation_properties
from {{ ref("int_dac2a_mapped") }}
