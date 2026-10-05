{#-
    The denominator segment a ratio variable carries. Separate from
    test_mint_statvar_dcid because that one calls the macro without a denominator,
    which is what proves the addition left every existing caller alone.
-#}
with cases as (
    select cases.*
    from (
        values
        (
            'a denominator contributes a trailing AsAFractionOf segment',
            'amount', 'FinancialFlow', 'InterestPayment', null,
            'Amount_EconomicActivity_Exports',
            'dcid:Amount_FinancialFlow_InterestPayment_AsAFractionOf_Amount_EconomicActivity_Exports'  -- noqa: LT05
        ),
        (
            'a denominator sits behind the qualifier, not in front of it',
            'amount', 'FinancialPosition', 'AmountOutstanding', 'Nominal',
            'Count_Person',
            'dcid:Amount_FinancialPosition_AmountOutstanding_Nominal_AsAFractionOf_Count_Person'  -- noqa: LT05
        ),
        (
            'a prefixed denominator contributes its bare segment',
            'amount', 'FinancialFlow', 'InterestPayment', null, 'dcid:Count_Person',
            'dcid:Amount_FinancialFlow_InterestPayment_AsAFractionOf_Count_Person'
        ),
        (
            'no denominator leaves neither the marker nor a trailing separator',
            'amount', 'FinancialFlow', 'InterestPayment', 'Nominal', null,
            'dcid:Amount_FinancialFlow_InterestPayment_Nominal'
        ),
        (
            'a variable with neither qualifier nor denominator is unchanged',
            'amount', 'FinancialFlow', 'InterestPayment', null, null,
            'dcid:Amount_FinancialFlow_InterestPayment'
        )
    ) as cases (
        case_name,
        measured_property,
        population_type,
        constraint_one,
        measurement_qualifier,
        measurement_denominator,
        expected
    )
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
