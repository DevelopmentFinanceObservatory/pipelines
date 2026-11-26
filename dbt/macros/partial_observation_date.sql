{% macro partial_observation_date(dataset, release_date, latest_observation_date) -%}
    {#-
        The newest year held, as an ISO 8601 year, while the release predates
        the dataset's completing release in the following year; null once it
        is complete.
    -#}
    {%- set completing_release_month = var("release_calendars")[dataset].completing_release_month -%}
    case
        when {{ release_date }}
            < cast(
                cast(cast({{ latest_observation_date }} as integer) + 1 as string)
                || '-{{ "%02d" | format(completing_release_month) }}-01'
                as date
            )
            then {{ latest_observation_date }}
    end
{%- endmacro %}
