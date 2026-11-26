with cases as (
    select cases.*
    from (
        select
            'a commitment is named for its obligation type' as case_name,
            'other flow (CRS Flow)' as type_of_flow_label,
            'grant (CRS Finance)' as type_of_finance_label,
            'budget support (CRS Modality)' as cooperation_modality_label,
            'education (CRS Purpose)' as purpose_label,
            'dcid:amount' as measured_property,
            'dcid:Commitment' as financial_obligation_type,
            null as financial_flow_type,
            null as netting_basis,
            'Other flow commitments for education (grant, budget support)' as expected
        union all
        select
            'a gross disbursement is named for its netting basis' as case_name,
            'other flow (CRS Flow)' as type_of_flow_label,
            'grant (CRS Finance)' as type_of_finance_label,
            'budget support (CRS Modality)' as cooperation_modality_label,
            'education (CRS Purpose)' as purpose_label,
            'dcid:amount' as measured_property,
            null as financial_obligation_type,
            'dcid:Disbursement' as financial_flow_type,
            'dcid:Gross' as netting_basis,
            'Other flow gross disbursements for education (grant, budget support)' as expected
        union all
        select
            'a net disbursement is named for its netting basis' as case_name,
            'other flow (CRS Flow)' as type_of_flow_label,
            'grant (CRS Finance)' as type_of_finance_label,
            'budget support (CRS Modality)' as cooperation_modality_label,
            'education (CRS Purpose)' as purpose_label,
            'dcid:amount' as measured_property,
            null as financial_obligation_type,
            'dcid:Disbursement' as financial_flow_type,
            'dcid:Net' as netting_basis,
            'Other flow net disbursements for education (grant, budget support)' as expected
        union all
        select
            'a grant equivalent is named for its measured property' as case_name,
            'other flow (CRS Flow)' as type_of_flow_label,
            'grant (CRS Finance)' as type_of_finance_label,
            'budget support (CRS Modality)' as cooperation_modality_label,
            'education (CRS Purpose)' as purpose_label,
            'dcid:grantEquivalent' as measured_property,
            null as financial_obligation_type,
            'dcid:Disbursement' as financial_flow_type,
            null as netting_basis,
            'Other flow grant equivalents for education (grant, budget support)' as expected
        union all
        select
            'a modality that does not apply leaves no gap' as case_name,
            'other flow (CRS Flow)' as type_of_flow_label,
            'grant (CRS Finance)' as type_of_finance_label,
            null as cooperation_modality_label,
            'education (CRS Purpose)' as purpose_label,
            'dcid:amount' as measured_property,
            'dcid:Commitment' as financial_obligation_type,
            null as financial_flow_type,
            null as netting_basis,
            'Other flow commitments for education (grant)' as expected
        union all
        select
            'a label with no vocabulary to strip is taken whole' as case_name,
            'other flow' as type_of_flow_label,
            'grant' as type_of_finance_label,
            'budget support' as cooperation_modality_label,
            'education' as purpose_label,
            'dcid:amount' as measured_property,
            'dcid:Commitment' as financial_obligation_type,
            null as financial_flow_type,
            null as netting_basis,
            'Other flow commitments for education (grant, budget support)' as expected
        union all
        select
            'a group marker is kept, as it can be all that separates two codes' as case_name,
            'other flow (CRS Flow)' as type_of_flow_label,
            'grant (CRS Finance)' as type_of_finance_label,
            'core support (group) (CRS Modality)' as cooperation_modality_label,
            'education (CRS Purpose)' as purpose_label,
            'dcid:amount' as measured_property,
            'dcid:Commitment' as financial_obligation_type,
            null as financial_flow_type,
            null as netting_basis,
            'Other flow commitments for education (grant, core support (group))' as expected
    ) as cases
),

minted as (
    select
        cases.case_name,
        cases.expected,
        {{
            mint_crs_statvar_name(
                type_of_flow_label="cases.type_of_flow_label",
                type_of_finance_label="cases.type_of_finance_label",
                cooperation_modality_label="cases.cooperation_modality_label",
                purpose_label="cases.purpose_label",
                measured_property="cases.measured_property",
                financial_obligation_type="cases.financial_obligation_type",
                financial_flow_type="cases.financial_flow_type",
                netting_basis="cases.netting_basis"
            )
        }} as minted_name
    from cases
)

select
    minted.case_name,
    minted.expected,
    minted.minted_name
from minted
where minted.minted_name is distinct from minted.expected
