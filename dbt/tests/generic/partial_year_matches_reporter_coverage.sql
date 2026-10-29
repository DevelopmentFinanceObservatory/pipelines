{% test partial_year_matches_reporter_coverage(model, column_name, staged, reporter) %}
{#
    The provenance when its newest year has fewer than half the reporters of
    the year before, but no partial year is marked, or the other way round.
    Passes when the year before has no rows.

    A failure means a file was published off the calendar, or a year is filling
    faster or slower than the calendar expects. Check the release, then fix
    the calendar in release_calendars or wait for the next file.
#}
with provenance as (
    select
        cast(latest_observation_date as integer) as latest_year,
        {{ column_name }} as partial_year
    from {{ model }}
),

reporters as (
    select
        staged.year,
        count(distinct staged.{{ reporter }}) as reporter_count
    from {{ staged }} as staged
    inner join provenance
        on staged.year between provenance.latest_year - 1 and provenance.latest_year
    group by staged.year
)

select
    provenance.latest_year,
    provenance.partial_year,
    newest.reporter_count as newest_reporters,
    previous.reporter_count as previous_reporters
from provenance
inner join reporters as previous
    on provenance.latest_year - 1 = previous.year
left join reporters as newest
    on provenance.latest_year = newest.year
where
    (coalesce(newest.reporter_count, 0) * 2 < previous.reporter_count)
    != (provenance.partial_year is not null)
{% endtest %}
