{% macro replace_every_match(text, pattern, replacement) -%}
    {#- `text` with every match of `pattern` replaced, where DuckDB replaces the first alone. -#}
    {{ return(adapter.dispatch("replace_every_match")(text, pattern, replacement)) }}
{%- endmacro %}

{% macro duckdb__replace_every_match(text, pattern, replacement) -%}
    regexp_replace({{ text }}, {{ pattern }}, {{ replacement }}, 'g')
{%- endmacro %}

{% macro bigquery__replace_every_match(text, pattern, replacement) -%}
    regexp_replace({{ text }}, {{ pattern }}, {{ replacement }})
{%- endmacro %}
