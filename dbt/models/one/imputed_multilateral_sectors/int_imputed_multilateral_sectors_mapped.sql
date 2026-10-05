with
-- Every dimension except the purpose code is a constant literal. The series
-- covers ODA gross disbursements in current-price USD and measures
-- attributedAmount, the part of a flow's amount credited to a party whose
-- resources it draws on, where the flow cannot be traced to that party
-- directly. Here the flow is a multilateral organisation's spending and the
-- party is the donor, named by financingProvider, whose core contributions it
-- draws on. The dac2aAidType constraint (DAC2aAidType106) is part of the
-- StatVar dcid, and the Modelled measurement method says each value is
-- estimated. Only the purpose code and the entities resolved in
-- int_imputed_multilateral_sectors_resolved vary per row.
dc_names as (
    select
        aggregated.year,
        aggregated.donor as financing_provider,
        aggregated.recipient as financing_recipient,
        aggregated.value,
        'dcid:CurrencyUSD' as unit,
        'dcid:measuredValue' as stat_type,
        'dcid:Nominal' as measurement_qualifier,
        'dcid:attributedAmount' as measured_property,
        'dcid:FinancialFlow' as population_type,
        'dcid:Gross' as netting_basis,
        'dcid:Disbursement' as financial_flow_type,
        'dcid:CRSTypeOfFlow10' as type_of_flow,
        'dcid:DAC2aAidType106' as aid_type,
        -- measurement_method is a method node, not a constraint. Every value
        -- is Modelled. mint_statvar_dcid excludes it from the StatVar dcid,
        -- and it passes straight through to the observations.
        'dcid:Modelled' as measurement_method,
        purpose_codes.dcid as purpose_code,
        purpose_codes.label as purpose_label
    from {{ ref("int_imputed_multilateral_sectors_aggregated") }} as aggregated
    left join {{ ref("code_mapping") }} as purpose_codes
        on
            purpose_codes.list = 'dcid:CRSPurposeCodeEnum'
            and purpose_codes.property = 'classificationCode'
            and aggregated.purpose_code = purpose_codes.code
)

{% set constraints = {
    "nettingBasis": "dc_names.netting_basis",
    "financialFlowType": "dc_names.financial_flow_type",
    "crsTypeOfFlow": "dc_names.type_of_flow",
    "dac2aAidType": "dc_names.aid_type",
    "crsPurpose": "dc_names.purpose_code",
} %}

select
    {{
        mint_statvar_dcid(
            stat_type_dcid_segment="stat_types.dcid_segment",
            measured_property="dc_names.measured_property",
            population_type="dc_names.population_type",
            constraints=constraints,
            measurement_qualifier="dc_names.measurement_qualifier"
        )
    }} as statvar_dcid,
    {{
        mint_imputed_multilateral_sectors_statvar_name(
            purpose_label="dc_names.purpose_label"
        )
    }} as statvar_name,
    dc_names.population_type,
    dc_names.measured_property,
    dc_names.stat_type,
    {{ list_constraint_properties(constraints) }} as constraint_properties,
    dc_names.netting_basis,
    dc_names.financial_flow_type,
    dc_names.type_of_flow,
    dc_names.aid_type,
    dc_names.purpose_code,
    dc_names.measurement_qualifier,
    dc_names.measurement_method,
    dc_names.financing_provider,
    dc_names.financing_recipient,
    dc_names.unit,
    dc_names.year,
    dc_names.value
from dc_names
left join {{ ref("stat_type_mapping") }} as stat_types
    on dc_names.stat_type = stat_types.stat_type
