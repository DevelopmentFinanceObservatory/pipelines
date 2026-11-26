{% macro utc_date(timestamp) -%}
    {#- The UTC calendar date of a timestamp with time zone. -#}
    {{ return(adapter.dispatch("utc_date")(timestamp)) }}
{%- endmacro %}

{% macro duckdb__utc_date(timestamp) -%}
    cast({{ timestamp }} at time zone 'UTC' as date)
{%- endmacro %}

{% macro bigquery__utc_date(timestamp) -%}
    date({{ timestamp }}, 'UTC')
{%- endmacro %}
