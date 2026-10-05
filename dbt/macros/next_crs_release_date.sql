{% macro next_crs_release_date(release_date) -%}
    {#-
        OECD publishes CRS in April, June, September and December. This is the
        first of those months after the given release, rolling into April of
        the following year once December has been published. If a release is
        late, this holds at the month it targets rather than skipping ahead
        once that month has passed with nothing published.
    -#}
    case
        when extract(month from {{ release_date }}) < 4
            then cast(extract(year from {{ release_date }}) as varchar) || '-04'
        when extract(month from {{ release_date }}) < 6
            then cast(extract(year from {{ release_date }}) as varchar) || '-06'
        when extract(month from {{ release_date }}) < 9
            then cast(extract(year from {{ release_date }}) as varchar) || '-09'
        when extract(month from {{ release_date }}) < 12
            then cast(extract(year from {{ release_date }}) as varchar) || '-12'
        else cast(extract(year from {{ release_date }}) + 1 as varchar) || '-04'
    end
{%- endmacro %}
