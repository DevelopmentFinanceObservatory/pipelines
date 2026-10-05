{% macro weo_excluded_areas() -%}
    {#-
        WEO country groups, as a list for `not in`. They have no nodes to
        resolve to until the ontology mints IMF group nodes.
    -#}
    (
        'G001',
        'G110',
        'G119',
        'G163',
        'G200',
        'G205',
        'G400',
        'G505',
        'G510',
        'G603',
        'G903',
        'G998',
        'GX123'
    )
{%- endmacro %}
