{% macro matches_pattern(text, pattern) -%}
    {#- Whether `pattern` matches anywhere in `text`; anchor it to match the whole. -#}
    {{ return(adapter.dispatch("matches_pattern")(text, pattern)) }}
{%- endmacro %}

{% macro duckdb__matches_pattern(text, pattern) -%}
    regexp_matches({{ text }}, {{ pattern }})
{%- endmacro %}

{% macro bigquery__matches_pattern(text, pattern) -%}
    regexp_contains({{ text }}, {{ pattern }})
{%- endmacro %}
