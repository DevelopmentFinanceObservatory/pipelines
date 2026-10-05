with cases as (
    select cases.*
    from (
        values
        (
            'a commitment is named for its obligation type',
            'other flow (CRS Flow)', 'grant (CRS Finance)',
            'budget support (CRS Modality)', 'education (CRS Purpose)',
            'dcid:amount', 'dcid:Commitment', null, null,
            'Other flow commitments for education (grant, budget support)'
        ),
        (
            'a gross disbursement is named for its netting basis',
            'other flow (CRS Flow)', 'grant (CRS Finance)',
            'budget support (CRS Modality)', 'education (CRS Purpose)',
            'dcid:amount', null, 'dcid:Disbursement', 'dcid:Gross',
            'Other flow gross disbursements for education (grant, budget support)'
        ),
        (
            'a net disbursement is named for its netting basis',
            'other flow (CRS Flow)', 'grant (CRS Finance)',
            'budget support (CRS Modality)', 'education (CRS Purpose)',
            'dcid:amount', null, 'dcid:Disbursement', 'dcid:Net',
            'Other flow net disbursements for education (grant, budget support)'
        ),
        (
            'a grant equivalent is named for its measured property',
            'other flow (CRS Flow)', 'grant (CRS Finance)',
            'budget support (CRS Modality)', 'education (CRS Purpose)',
            'dcid:grantEquivalent', null, 'dcid:Disbursement', null,
            'Other flow grant equivalents for education (grant, budget support)'
        ),
        (
            'a modality that does not apply leaves no gap',
            'other flow (CRS Flow)', 'grant (CRS Finance)',
            null, 'education (CRS Purpose)',
            'dcid:amount', 'dcid:Commitment', null, null,
            'Other flow commitments for education (grant)'
        ),
        (
            'a label with no vocabulary to strip is taken whole',
            'other flow', 'grant',
            'budget support', 'education',
            'dcid:amount', 'dcid:Commitment', null, null,
            'Other flow commitments for education (grant, budget support)'
        ),
        (
            'a group marker is kept, as it can be all that separates two codes',
            'other flow (CRS Flow)', 'grant (CRS Finance)',
            'core support (group) (CRS Modality)', 'education (CRS Purpose)',
            'dcid:amount', 'dcid:Commitment', null, null,
            'Other flow commitments for education (grant, core support (group))'
        )
    ) as cases (
        case_name,
        type_of_flow_label,
        type_of_finance_label,
        cooperation_modality_label,
        purpose_label,
        measured_property,
        financial_obligation_type,
        financial_flow_type,
        netting_basis,
        expected
    )
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
