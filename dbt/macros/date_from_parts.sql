{% macro date_from_parts(year, month, day) -%}
    {#- The date of a year, month and day, which BigQuery builds with `date`. -#}
    {{ return(adapter.dispatch("date_from_parts")(year, month, day)) }}
{%- endmacro %}

{% macro duckdb__date_from_parts(year, month, day) -%}
    make_date({{ year }}, {{ month }}, {{ day }})
{%- endmacro %}

{% macro bigquery__date_from_parts(year, month, day) -%}
    date({{ year }}, {{ month }}, {{ day }})
{%- endmacro %}
