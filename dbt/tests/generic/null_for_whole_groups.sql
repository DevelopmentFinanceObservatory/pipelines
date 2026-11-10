{% test null_for_whole_groups(model, column_name, group_by) %}
{#
    The groups of `group_by` columns in which `column_name` is null on some rows
    and not on others. Passes when every group has the column on all its rows or
    on none.

    A model that drops the null rows then drops whole groups and publishes no
    partial total.
#}
select
    {{ group_by | join(", ") }},
    count(*) as row_count,
    count({{ column_name }}) as non_null_count
from {{ model }}
group by {{ group_by | join(", ") }}
having count({{ column_name }}) not in (0, count(*))
{% endtest %}
