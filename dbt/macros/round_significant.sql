{% macro round_significant(expression) -%}
    {#-
        The value as a double of at most 15 significant digits, all a Data Commons value carries
    -#}
    cast(printf('%.15g', {{ expression }}) as double)
{%- endmacro %}
