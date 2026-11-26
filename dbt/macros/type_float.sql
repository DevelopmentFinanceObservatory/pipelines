{% macro duckdb__type_float() -%}
    {#- A 64-bit float, as `float` is on other warehouses; DuckDB reads `float` as 32-bit -#}
    {{ return("double") }}
{%- endmacro %}
