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
    financial_obligation_type,
    netting_basis,
    fixed_capital_consumption_basis,
    dac_type_of_finance,
    dac1_aid_type,
    case
        when entity_property = 'financingProvider' and recipient is null
            then 'financingProvider'
        when entity_property = 'financingProvider'
            then 'financingProvider,financingRecipient'
    end as observation_properties
from {{ ref("int_dac1_mapped") }}
where defines_statvar
