{% macro ids_year_columns() -%}
    {#-
        The raw IDS table's year columns, one per year and named for it, quoted as a
        list for `unpivot … in`. Unit tests override it, since their inputs have no table
        to read the columns from.
    -#}
    {%- set year_columns = [] -%}
    {%- for column in adapter.get_columns_in_relation(source("world_bank_ids", "raw_ids")) -%}
        {%- if column.name | length == 4 and column.name.isdigit() -%}
            {%- do year_columns.append(adapter.quote(column.name)) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ year_columns | join(", ") }}
{%- endmacro %}
