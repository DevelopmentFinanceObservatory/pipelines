-- Every StatVar the three imports publish, one row each. Each import's
-- StatVar model selects its own from here, so a StatVar two imports share,
-- ONE Data's and the OECD's received commitments, is defined byte for byte
-- alike in both bundles.
select distinct
    statvar_dcid as dcid,
    statvar_name as name,
    population_type,
    measured_property,
    stat_type,
    constraint_properties,
    netting_basis,
    financial_flow_type,
    financial_obligation_type,
    climate_policy_objective,
    measurement_qualifier,
    -- The UNFCCC totals cover every recipient, so their observations carry
    -- the provider alone.
    case
        when financing_recipient is null then 'financingProvider'
        else 'financingProvider,financingRecipient'
    end as observation_properties
from {{ ref("int_climate_finance_mapped") }}
