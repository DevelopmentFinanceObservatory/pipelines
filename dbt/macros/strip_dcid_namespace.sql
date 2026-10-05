{% macro strip_dcid_namespace(dcid) -%}
    {#-
        A DCID with its namespace prefix removed e.g. `dcid:MyNode` -> `MyNode`
    -#}
    regexp_replace({{ dcid }}, '^.*:', '')
{%- endmacro %}
