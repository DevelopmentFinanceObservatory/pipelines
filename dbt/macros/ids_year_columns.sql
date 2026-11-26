{% macro ids_year_columns() -%}
    {#-
        The names of the raw IDS table's year columns, one per year and named for it.
        Unit tests override it, since their inputs have no table to read the columns
        from. With no raw table, as when linting, the list is empty.
    -#}
    {%- set year_columns = [] -%}
    {%- for column in adapter.get_columns_in_relation(source("world_bank_ids", "raw_ids")) -%}
        {%- if column.name | length == 4 and column.name.isdigit() -%}
            {%- do year_columns.append(column.name) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- do return(year_columns) -%}
{%- endmacro %}
