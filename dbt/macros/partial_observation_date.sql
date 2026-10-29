{% macro partial_observation_date(dataset, release_date, latest_observation_date) -%}
    {#-
        The newest year held, as an ISO 8601 year, while the release predates
        the dataset's completing release in the following year; null once it
        is complete.
    -#}
    case
        when {{ release_date }}
            < make_date(
                cast({{ latest_observation_date }} as integer) + 1,
                {{ var("release_calendars")[dataset].completing_release_month }},
                1
            )
            then {{ latest_observation_date }}
    end
{%- endmacro %}
