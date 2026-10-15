{% macro exclude_incomplete_years(import_name) -%}
    {#- A where-clause condition dropping the years incomplete_years lists for an import. -#}
    {%- set years = var("incomplete_years", {}).get(import_name, []) -%}
    {%- if years -%} year not in ({{ years | join(", ") }}) {%- else -%} true {%- endif -%}
{%- endmacro %}
