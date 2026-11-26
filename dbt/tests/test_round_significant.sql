with cases as (
    select cases.*
    from (
        select
            'float noise from a scaled value is dropped' as case_name,
            cast(13964518865999.998 as {{ dbt.type_float() }}) as input_value,
            '13964518866000.0' as expected
        union all
        select
            'a small value keeps its exponent' as case_name,
            cast(3.798584854685877e-09 as {{ dbt.type_float() }}) as input_value,
            '3.79858485468588e-09' as expected
        union all
        select
            'a large value keeps its exponent' as case_name,
            cast(2.2841494455984202e+18 as {{ dbt.type_float() }}) as input_value,
            '2.28414944559842e+18' as expected
        union all
        select
            'a 15-digit value is kept whole' as case_name,
            cast(5.55759999999999 as {{ dbt.type_float() }}) as input_value,
            '5.55759999999999' as expected
        union all
        select
            'float noise from a sum is dropped' as case_name,
            cast(0.1 as {{ dbt.type_float() }}) + cast(0.2 as {{ dbt.type_float() }}) as input_value,
            '0.3' as expected
        union all
        select
            'a negative keeps its sign' as case_name,
            cast(-0.000311 as {{ dbt.type_float() }}) as input_value,
            '-0.000311' as expected
        union all
        select
            'zero stays zero' as case_name,
            cast(0 as {{ dbt.type_float() }}) as input_value,
            '0.0' as expected
        union all
        select
            'a double rounded to cents keeps its cents' as case_name,
            round(cast(123456789.123456789 as {{ dbt.type_float() }}), 2) as input_value,
            '123456789.12' as expected
        union all
        select
            'a null stays null' as case_name,
            cast(null as {{ dbt.type_float() }}) as input_value,
            null as expected
    ) as cases
),

rounded as (
    select
        cases.case_name,
        cases.expected,
        {{ round_significant("cases.input_value") }} as rounded_value
    from cases
)

select
    rounded.case_name,
    rounded.expected,
    rounded.rounded_value
from rounded
where rounded.rounded_value is distinct from cast(rounded.expected as {{ dbt.type_float() }})
