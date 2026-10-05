{% macro conversion_tables_rebased_deflators(deflators_unrebased) -%}
    {#-
        One conversion table's deflators, `deflators_unrebased`, with each
        entity's series rebased to equal 100 in the deflator base year. A rebase
        divides every year by the same number, so it leaves every ratio of two
        years, and so every converted value, unchanged. An entity with no deflator
        in the base year cannot be rebased and is left out. A rebased year is an
        estimate when either it or the base year is, because its value depends on
        both.
    -#}
    with base_year_deflators as (
        select
            entity,
            deflator as base_year_deflator,
            is_estimate as base_year_is_estimate
        from {{ deflators_unrebased }}
        where year = {{ var("conversion_tables_deflator_base_year") }}
    )

    select
        deflators.entity,
        deflators.year,
        deflators.is_estimate or base_year_deflators.base_year_is_estimate
            as is_estimate,
        100 * deflators.deflator / base_year_deflators.base_year_deflator
            as deflator
    from {{ deflators_unrebased }} as deflators
    inner join base_year_deflators
        on deflators.entity = base_year_deflators.entity
{%- endmacro %}
