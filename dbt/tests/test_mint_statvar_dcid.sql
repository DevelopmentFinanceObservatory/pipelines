{#-
    The cases in `minted` call the macro without a scope segment and pin the
    DCIDs it mints for callers that pass none. The cases in `scoped_minted`
    pass one.
-#}
with cases as (
    select cases.*
    from (
        values
        (
            'a stat type contributing no segment leaves none behind',
            null, 'amount', 'FinancialFlow', 'ODA', 'Gross', 'Nominal',
            'dcid:Amount_FinancialFlow_ODA_Gross_Nominal'
        ),
        (
            'a stat type segment is taken as curated, not recased',
            'Sum', 'amount', 'FinancialFlow', 'ODA', 'Gross', 'Nominal',
            'dcid:Sum_Amount_FinancialFlow_ODA_Gross_Nominal'
        ),
        (
            'a constraint that does not apply leaves no gap',
            null, 'grantEquivalent', 'FinancialFlow', null, 'Gross', 'Nominal',
            'dcid:GrantEquivalent_FinancialFlow_Gross_Nominal'
        ),
        (
            'a prefixed constraint contributes its bare segment',
            null, 'amount', 'FinancialFlow', 'dcid:ODA', 'Gross', 'Nominal',
            'dcid:Amount_FinancialFlow_ODA_Gross_Nominal'
        ),
        (
            'a prefixed qualifier contributes its bare segment',
            null, 'amount', 'FinancialFlow', 'ODA', 'Gross', 'dcid:Nominal',
            'dcid:Amount_FinancialFlow_ODA_Gross_Nominal'
        ),
        (
            'a missing final segment leaves no trailing separator',
            null, 'amount', 'FinancialFlow', 'ODA', 'Gross', null,
            'dcid:Amount_FinancialFlow_ODA_Gross'
        )
    ) as cases (
        case_name,
        stat_type_dcid_segment,
        measured_property,
        population_type,
        constraint_one,
        constraint_two,
        measurement_qualifier,
        expected
    )
),

minted as (
    select
        cases.case_name,
        cases.expected,
        {{
            mint_statvar_dcid(
                stat_type_dcid_segment="cases.stat_type_dcid_segment",
                measured_property="cases.measured_property",
                population_type="cases.population_type",
                constraints={
                    "constraintOne": "cases.constraint_one",
                    "constraintTwo": "cases.constraint_two",
                },
                measurement_qualifier="cases.measurement_qualifier"
            )
        }} as minted_dcid
    from cases
),

scoped_cases as (
    select scoped_cases.*
    from (
        values
        (
            'a scope segment sits between the constraints and the qualifier',
            'amount', 'FinancialObligation', 'Commitment',
            'ClimateChangePolicyObjective', 'AllRecipients', 'Nominal',
            'dcid:Amount_FinancialObligation_Commitment_ClimateChangePolicyObjective_AllRecipients_Nominal'  -- noqa: LT05
        ),
        (
            'a null scope segment leaves no gap',
            'amount', 'FinancialObligation', 'Commitment',
            'ClimateChangePolicyObjective', null, 'Nominal',
            'dcid:Amount_FinancialObligation_Commitment_ClimateChangePolicyObjective_Nominal'  -- noqa: LT05
        )
    ) as scoped_cases (
        case_name,
        measured_property,
        population_type,
        constraint_one,
        constraint_two,
        scope_segment,
        measurement_qualifier,
        expected
    )
),

scoped_minted as (
    select
        scoped_cases.case_name,
        scoped_cases.expected,
        {{
            mint_statvar_dcid(
                stat_type_dcid_segment="null",
                measured_property="scoped_cases.measured_property",
                population_type="scoped_cases.population_type",
                constraints={
                    "constraintOne": "scoped_cases.constraint_one",
                    "constraintTwo": "scoped_cases.constraint_two",
                },
                measurement_qualifier="scoped_cases.measurement_qualifier",
                scope_segment="scoped_cases.scope_segment"
            )
        }} as minted_dcid
    from scoped_cases
)

select
    minted.case_name,
    minted.expected,
    minted.minted_dcid
from minted
where minted.minted_dcid is distinct from minted.expected

union all

select
    scoped_minted.case_name,
    scoped_minted.expected,
    scoped_minted.minted_dcid
from scoped_minted
where scoped_minted.minted_dcid is distinct from scoped_minted.expected
