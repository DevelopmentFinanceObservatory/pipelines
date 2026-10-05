{% macro strip_label_parenthetical(label) -%}
    {#-
        A label with its trailing parenthetical removed e.g.
        `standard grant (CRS Finance)` -> `standard grant`.
    -#}
    regexp_replace({{ label }}, ' \([^)]*\)$', '')
{%- endmacro %}
