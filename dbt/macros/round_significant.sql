{% macro round_significant(expression) -%}
    {#-
        The value as a double of at most 15 significant digits, all a Data Commons value carries
    -#}
    {{ return(adapter.dispatch("round_significant")(expression)) }}
{%- endmacro %}

{% macro duckdb__round_significant(expression) -%}
    cast(printf('%.15g', {{ expression }}) as double)
{%- endmacro %}

{% macro bigquery__round_significant(expression) -%}
    cast(format('%.15g', {{ expression }}) as float64)
{%- endmacro %}
