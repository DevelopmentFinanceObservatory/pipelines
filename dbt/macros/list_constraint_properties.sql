{% macro list_constraint_properties(constraints) %}
    {#-
        The properties that constrain a StatVar, as a comma-delimited list in the
        order the model declared them. A property whose value is null does not
        constrain the variable, so it is left out of the list rather than named with
        nothing behind it.
    -#}
    array_to_string(
        [
            {%- for property, value in constraints.items() %}
            case when {{ value }} is not null then '{{ property }}' end
            {%- if not loop.last %},{% endif %}
            {%- endfor %}
        ],
        ','
    )
{%- endmacro %}
