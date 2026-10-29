{% macro partial_observation_date(dataset, release_date, latest_observation_date) -%}
    {#-
        The newest year held, as an ISO 8601 year, while the release predates
        the release in the dataset's calendar that completes it in the following year; null
        once it is complete.
    -#}
    {%- set completing_months = [] -%}
    {%- for release in var("release_calendars")[dataset] if release.get("completes_previous_year") -%}
        {%- do completing_months.append(release.month) -%}
    {%- endfor -%}
    {%- if completing_months | length != 1 -%}
        {{ exceptions.raise_compiler_error(
            "release_calendars." ~ dataset ~ " must flag exactly one month completes_previous_year"
        ) }}
    {%- endif -%}
    case
        when {{ release_date }}
            < make_date(cast({{ latest_observation_date }} as integer) + 1, {{ completing_months[0] }}, 1)
            then {{ latest_observation_date }}
    end
{%- endmacro %}
