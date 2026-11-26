with cases as (
    select cases.*
    from (
        values
        (
            'float noise from a scaled value is dropped',
            cast(13964518865999.998 as double), '13964518866000.0'
        ),
        (
            'a small value keeps its exponent',
            cast(3.798584854685877e-09 as double), '3.79858485468588e-09'
        ),
        (
            'a large value keeps its exponent',
            cast(2.2841494455984202e+18 as double), '2.28414944559842e+18'
        ),
        (
            'a 15-digit value is kept whole',
            cast(5.55759999999999 as double), '5.55759999999999'
        ),
        (
            'float noise from a sum is dropped',
            cast(0.1 as double) + cast(0.2 as double), '0.3'
        ),
        (
            'a negative keeps its sign',
            cast(-0.000311 as double), '-0.000311'
        ),
        (
            'zero stays zero',
            cast(0 as double), '0.0'
        ),
        (
            'a double rounded to cents keeps its cents',
            round(cast(123456789.123456789 as double), 2), '123456789.12'
        )
    ) as cases (case_name, input_value, expected)
),

rounded as (
    select
        cases.case_name,
        cases.expected,
        cast({{ round_significant("cases.input_value") }} as string) as rounded_value
    from cases
)

select
    rounded.case_name,
    rounded.expected,
    rounded.rounded_value
from rounded
where rounded.rounded_value is distinct from rounded.expected

union all

select
    'a null stays null' as case_name,
    'null' as expected,
    'not null' as rounded_value
where {{ round_significant("cast(null as double)") }} is not null
