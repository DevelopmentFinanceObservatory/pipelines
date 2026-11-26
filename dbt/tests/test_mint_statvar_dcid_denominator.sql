{#-
    The denominator segment a ratio variable carries. Separate from
    test_mint_statvar_dcid because that one calls the macro without a denominator,
    which is what proves the addition left every existing caller alone.
-#}
with cases as (
    select cases.*
    from (
        select
            'a denominator contributes a trailing AsAFractionOf segment' as case_name,
            'amount' as measured_property,
            'FinancialFlow' as population_type,
            'InterestPayment' as constraint_one,
            null as measurement_qualifier,
            'Amount_EconomicActivity_Exports' as measurement_denominator,
            'dcid:Amount_FinancialFlow_InterestPayment_AsAFractionOf_Amount_EconomicActivity_Exports' as expected  -- noqa: LT05
        union all
        select
            'a denominator sits behind the qualifier, not in front of it' as case_name,
            'amount' as measured_property,
            'FinancialPosition' as population_type,
            'AmountOutstanding' as constraint_one,
            'Nominal' as measurement_qualifier,
            'Count_Person' as measurement_denominator,
            'dcid:Amount_FinancialPosition_AmountOutstanding_Nominal_AsAFractionOf_Count_Person' as expected  -- noqa: LT05
        union all
        select
            'a prefixed denominator contributes its bare segment' as case_name,
            'amount' as measured_property,
            'FinancialFlow' as population_type,
            'InterestPayment' as constraint_one,
            null as measurement_qualifier,
            'dcid:Count_Person' as measurement_denominator,
            'dcid:Amount_FinancialFlow_InterestPayment_AsAFractionOf_Count_Person' as expected
        union all
        select
            'no denominator leaves neither the marker nor a trailing separator' as case_name,
            'amount' as measured_property,
            'FinancialFlow' as population_type,
            'InterestPayment' as constraint_one,
            'Nominal' as measurement_qualifier,
            null as measurement_denominator,
            'dcid:Amount_FinancialFlow_InterestPayment_Nominal' as expected
        union all
        select
            'a variable with neither qualifier nor denominator is unchanged' as case_name,
            'amount' as measured_property,
            'FinancialFlow' as population_type,
            'InterestPayment' as constraint_one,
            null as measurement_qualifier,
            null as measurement_denominator,
            'dcid:Amount_FinancialFlow_InterestPayment' as expected
    ) as cases
),

minted as (
    select
        cases.case_name,
        cases.expected,
        {{
            mint_statvar_dcid(
                stat_type_dcid_segment="null",
                measured_property="cases.measured_property",
                population_type="cases.population_type",
                constraints={"constraintOne": "cases.constraint_one"},
                measurement_qualifier="cases.measurement_qualifier",
                measurement_denominator="cases.measurement_denominator"
            )
        }} as minted_dcid
    from cases
)

select
    minted.case_name,
    minted.expected,
    minted.minted_dcid
from minted
where minted.minted_dcid is distinct from minted.expected
