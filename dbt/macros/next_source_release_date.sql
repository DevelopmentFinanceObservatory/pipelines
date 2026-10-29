{% macro next_source_release_date(calendar, release_date) -%}
    {#-
        The first month in the release calendar after the given release,
        rolling into the calendar's first month of the following year once its
        last has been published. If a release is late, this holds at the month
        it targets rather than skipping ahead once that month has passed with
        nothing published.
    -#}
    {%- set months = var("release_calendars")[calendar] | map(attribute="month") | sort -%}
    case
        {%- for month in months %}
        when extract(month from {{ release_date }}) < {{ month }}
            then cast(extract(year from {{ release_date }}) as varchar)
                || '-{{ "%02d" | format(month) }}'
        {%- endfor %}
        else cast(extract(year from {{ release_date }}) + 1 as varchar)
            || '-{{ "%02d" | format(months[0]) }}'
    end
{%- endmacro %}
