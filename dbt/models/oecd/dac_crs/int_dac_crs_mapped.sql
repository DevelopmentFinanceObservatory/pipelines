with
-- Map to DCID for each dimension.
dc_names as (
    select
        unified.donor as financing_provider,
        unified.recipient as financing_recipient,
        unified.year,
        unified.value,
        'dcid:CurrencyUSD' as unit,
        'dcid:measuredValue' as stat_type,
        'dcid:Nominal' as measurement_qualifier,
        flow_codes.dcid as type_of_flow,
        finance_codes.dcid as type_of_finance,
        modality_codes.dcid as cooperation_modality,
        purpose_codes.dcid as purpose_code,
        flow_codes.label as type_of_flow_label,
        finance_codes.label as type_of_finance_label,
        modality_codes.label as cooperation_modality_label,
        purpose_codes.label as purpose_label,
        case unified.valuation_basis
            when 'grant_equivalent' then 'dcid:grantEquivalent'
            else 'dcid:amount'
        end as measured_property,
        case unified.financing_event_type
            when 'commitment' then 'dcid:FinancialObligation'
            when 'disbursement' then 'dcid:FinancialFlow'
        end as population_type,
        case unified.financing_event_type
            when 'commitment' then 'dcid:Commitment'
        end as financial_obligation_type,
        case unified.financing_event_type
            when 'disbursement' then 'dcid:Disbursement'
        end as financial_flow_type,
        case
            when unified.financing_event_type = 'disbursement' then
                case unified.netting_basis
                    when 'gross' then 'dcid:Gross'
                    when 'net' then 'dcid:Net'
                end
        end as netting_basis
    from {{ ref("int_dac_crs_unified") }} as unified
    left join {{ ref("code_mapping") }} as flow_codes
        on
            flow_codes.list = 'dcid:CRSTypeOfFlowEnum'
            and flow_codes.property = 'classificationCode'
            and unified.category = flow_codes.code
    left join {{ ref("code_mapping") }} as finance_codes
        on
            finance_codes.list = 'dcid:CRSTypeOfFinanceEnum'
            and finance_codes.property = 'classificationCode'
            and unified.finance_t = finance_codes.code
    left join {{ ref("code_mapping") }} as modality_codes
        on
            modality_codes.list = 'dcid:CRSCooperationModalityEnum'
            and modality_codes.property = 'classificationCode'
            and unified.aid_t = modality_codes.code
    left join {{ ref("code_mapping") }} as purpose_codes
        on
            purpose_codes.list = 'dcid:CRSPurposeCodeEnum'
            and purpose_codes.property = 'classificationCode'
            and unified.purpose_code = purpose_codes.code
)

{% set constraints = {
    "nettingBasis": "dc_names.netting_basis",
    "financialFlowType": "dc_names.financial_flow_type",
    "financialObligationType": "dc_names.financial_obligation_type",
    "crsTypeOfFlow": "dc_names.type_of_flow",
    "crsTypeOfFinance": "dc_names.type_of_finance",
    "crsCooperationModality": "dc_names.cooperation_modality",
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
        mint_crs_statvar_name(
            type_of_flow_label="dc_names.type_of_flow_label",
            type_of_finance_label="dc_names.type_of_finance_label",
            cooperation_modality_label="dc_names.cooperation_modality_label",
            purpose_label="dc_names.purpose_label",
            measured_property="dc_names.measured_property",
            financial_obligation_type="dc_names.financial_obligation_type",
            financial_flow_type="dc_names.financial_flow_type",
            netting_basis="dc_names.netting_basis"
        )
    }} as statvar_name,
    dc_names.population_type,
    dc_names.measured_property,
    dc_names.stat_type,
    {{ list_constraint_properties(constraints) }} as constraint_properties,
    dc_names.type_of_flow,
    dc_names.type_of_finance,
    dc_names.cooperation_modality,
    dc_names.purpose_code,
    dc_names.financial_obligation_type,
    dc_names.financial_flow_type,
    dc_names.netting_basis,
    dc_names.measurement_qualifier,
    dc_names.financing_provider,
    dc_names.financing_recipient,
    dc_names.unit,
    dc_names.year,
    dc_names.value
from dc_names
left join {{ ref("stat_type_mapping") }} as stat_types
    on dc_names.stat_type = stat_types.stat_type
