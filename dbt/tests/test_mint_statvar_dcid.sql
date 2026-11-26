{#-
    The cases in `minted` call the macro without a scope segment and pin the
    DCIDs it mints for callers that pass none. The cases in `scoped_minted`
    pass one.
-#}
with cases as (
    select cases.*
    from (
        select
            'a stat type contributing no segment leaves none behind' as case_name,
            null as stat_type_dcid_segment,
            'amount' as measured_property,
            'FinancialFlow' as population_type,
            'ODA' as constraint_one,
            'Gross' as constraint_two,
            'Nominal' as measurement_qualifier,
            'dcid:Amount_FinancialFlow_ODA_Gross_Nominal' as expected
        union all
        select
            'a stat type segment is taken as curated, not recased' as case_name,
            'Sum' as stat_type_dcid_segment,
            'amount' as measured_property,
            'FinancialFlow' as population_type,
            'ODA' as constraint_one,
            'Gross' as constraint_two,
            'Nominal' as measurement_qualifier,
            'dcid:Sum_Amount_FinancialFlow_ODA_Gross_Nominal' as expected
        union all
        select
            'a constraint that does not apply leaves no gap' as case_name,
            null as stat_type_dcid_segment,
            'grantEquivalent' as measured_property,
            'FinancialFlow' as population_type,
            null as constraint_one,
            'Gross' as constraint_two,
            'Nominal' as measurement_qualifier,
            'dcid:GrantEquivalent_FinancialFlow_Gross_Nominal' as expected
        union all
        select
            'a prefixed constraint contributes its bare segment' as case_name,
            null as stat_type_dcid_segment,
            'amount' as measured_property,
            'FinancialFlow' as population_type,
            'dcid:ODA' as constraint_one,
            'Gross' as constraint_two,
            'Nominal' as measurement_qualifier,
            'dcid:Amount_FinancialFlow_ODA_Gross_Nominal' as expected
        union all
        select
            'a prefixed qualifier contributes its bare segment' as case_name,
            null as stat_type_dcid_segment,
            'amount' as measured_property,
            'FinancialFlow' as population_type,
            'ODA' as constraint_one,
            'Gross' as constraint_two,
            'dcid:Nominal' as measurement_qualifier,
            'dcid:Amount_FinancialFlow_ODA_Gross_Nominal' as expected
        union all
        select
            'a missing final segment leaves no trailing separator' as case_name,
            null as stat_type_dcid_segment,
            'amount' as measured_property,
            'FinancialFlow' as population_type,
            'ODA' as constraint_one,
            'Gross' as constraint_two,
            null as measurement_qualifier,
            'dcid:Amount_FinancialFlow_ODA_Gross' as expected
    ) as cases
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
        select
            'a scope segment sits between the constraints and the qualifier' as case_name,
            'amount' as measured_property,
            'FinancialObligation' as population_type,
            'Commitment' as constraint_one,
            'ClimateChangePolicyObjective' as constraint_two,
            'AllRecipients' as scope_segment,
            'Nominal' as measurement_qualifier,
            'dcid:Amount_FinancialObligation_Commitment_ClimateChangePolicyObjective_AllRecipients_Nominal' as expected  -- noqa: LT05
        union all
        select
            'a null scope segment leaves no gap' as case_name,
            'amount' as measured_property,
            'FinancialObligation' as population_type,
            'Commitment' as constraint_one,
            'ClimateChangePolicyObjective' as constraint_two,
            null as scope_segment,
            'Nominal' as measurement_qualifier,
            'dcid:Amount_FinancialObligation_Commitment_ClimateChangePolicyObjective_Nominal' as expected  -- noqa: LT05
    ) as scoped_cases
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
