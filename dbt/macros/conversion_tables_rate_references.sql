{% macro conversion_tables_rate_references(recipe, rate_members) -%}
    {#-
        The area whose rate is each currency's rate in one conversion table, one
        row per currency. `recipe` is the table's ConversionRecipe member, which
        selects its rows of currency_rate_reference, and `rate_members` is its
        model of member rates. The two tables apply the same rule to different
        sources, and each builds its own copy so that neither table's failures
        hold back the other.

        A currency with one member area takes that area. A currency several areas
        use takes the area its currency_rate_reference row names, which must be
        one of them. A shared currency with no such row, or with a row naming an
        area that does not use it, gets a null reference_area, which the not_null
        test catches.
    -#}
    with member_areas as (
        select distinct
            currency,
            area
        from {{ rate_members }}
    ),

    currencies as (
        select
            currency,
            count(*) as member_count,
            min(area) as first_area
        from member_areas
        group by currency
    ),

    seeded_references as (
        select
            currency,
            area
        from {{ ref("currency_rate_reference") }}
        where
            recipe = '{{ recipe }}'
            and role = 'reference'
    )

    select
        currencies.currency,
        currencies.member_count,
        seeded_references.area as seeded_reference_area,
        case
            when seeded_references.area is not null then seeded_members.area
            when currencies.member_count = 1 then currencies.first_area
        end as reference_area
    from currencies
    left join seeded_references
        on currencies.currency = seeded_references.currency
    left join member_areas as seeded_members
        on
            seeded_references.currency = seeded_members.currency
            and seeded_references.area = seeded_members.area
{%- endmacro %}
