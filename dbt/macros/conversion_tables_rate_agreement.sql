{% macro conversion_tables_rate_agreement(recipe, rate_members, rate_references, rates) -%}
    {#-
        Every member area of a currency other than its reference, compared with
        the reference in the member's last actual year, for one conversion table.
        `recipe` selects the table's rows of currency_rate_reference, the three
        models are the table's own. A member agrees when its rate is within a
        relative 1e-6 of the reference's: the DAC1 bulk file carries
        national-currency amounts to six decimals and real members agree within
        1e-8, so 1e-6 leaves headroom while catching a wrong currency. Each
        table checks its own members. A DAC member's failure stops the DAC
        export only. An IMF member's failure stops both exports, because the
        DAC table fills from the IMF table's rates.

        The IMF map is present-tense, so the check reads the member's latest
        reported year. Earlier years can sit in a legacy currency (a euro member
        before it joined), and projected years follow each country's own GDP
        forecast. A member with a not_checked row in currency_rate_reference is
        left out, and the row says why.
    -#}
    with exempt_members as (
        select
            currency,
            area
        from {{ ref("currency_rate_reference") }}
        where
            recipe = '{{ recipe }}'
            and role = 'not_checked'
    ),

    last_actual_years as (
        select
            currency,
            area,
            max(year) filter (where not is_estimate) as last_actual_year
        from {{ rate_members }}
        group by currency, area
    ),

    checked_members as (
        select
            last_actual_years.currency,
            last_actual_years.area,
            last_actual_years.last_actual_year
        from last_actual_years
        inner join {{ rate_references }} as rate_references
            on
                last_actual_years.currency = rate_references.currency
                and last_actual_years.area <> rate_references.reference_area
        left join exempt_members
            on
                last_actual_years.currency = exempt_members.currency
                and last_actual_years.area = exempt_members.area
        where exempt_members.area is null
    )

    select
        checked_members.currency,
        checked_members.area,
        checked_members.last_actual_year,
        members.rate as member_rate,
        rates.rate as reference_rate,
        abs(members.rate / rates.rate - 1) as relative_deviation,
        1e-6 as tolerance
    from checked_members
    left join {{ rate_members }} as members
        on
            checked_members.area = members.area
            and checked_members.last_actual_year = members.year
    left join {{ rates }} as rates
        on
            checked_members.currency = rates.currency
            and checked_members.last_actual_year = rates.year
{%- endmacro %}
