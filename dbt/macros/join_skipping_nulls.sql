{% macro join_skipping_nulls(separator) -%}
    {#-
        The comma-separated expressions in the call block joined by `separator`,
        skipping each null along with its separator. DuckDB keeps `concat_ws`
        because its `array_to_string` returns null when every expression is null.
    -#}
    {{ return(adapter.dispatch("join_skipping_nulls")(separator, caller())) }}
{%- endmacro %}

{% macro duckdb__join_skipping_nulls(separator, expressions) -%}
    concat_ws(
        {{ separator }},
        {{ expressions }}
    )
{%- endmacro %}

{% macro bigquery__join_skipping_nulls(separator, expressions) -%}
    array_to_string(
        [
            {{ expressions }}
        ],
        {{ separator }}
    )
{%- endmacro %}
