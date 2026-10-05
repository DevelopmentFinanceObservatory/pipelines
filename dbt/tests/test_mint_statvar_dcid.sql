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
)

select
    minted.case_name,
    minted.expected,
    minted.minted_dcid
from minted
where minted.minted_dcid is distinct from minted.expected
