{% test partial_year_matches_reporter_coverage(model, column_name, staged, reporter) %}
{#
    Each of the three years ending at the provenance's newest year whose
    reporter coverage disagrees with the year the calendar flags as partial.

    A year is thin when fewer than half the most reporters of any of those
    years report in it; the half mirrors ANCHOR_SHARE in spec-kit's
    dfo_frame_grammar/rolling.py. The flagged year must be thin and every other
    year must not be, so a release holding two partial years fails too. A year
    with no staged rows is skipped rather than counted as thin.

    A failure means a file was published off the calendar, or a year is filling
    faster or slower than the calendar expects. Check the release, then fix
    the calendar in release_calendars or wait for the next file.
#}
with provenance as (
    select
        cast(latest_observation_date as integer) as latest_year,
        cast({{ column_name }} as integer) as partial_year
    from {{ model }}
),

reporters as (
    select
        staged.year,
        count(distinct staged.{{ reporter }}) as reporter_count
    from {{ staged }} as staged
    inner join provenance
        on staged.year between provenance.latest_year - 2 and provenance.latest_year
    group by staged.year
),

coverage as (
    select
        year,
        reporter_count,
        max(reporter_count) over () as most_reporters
    from reporters
)

select
    coverage.year,
    coverage.reporter_count,
    coverage.most_reporters,
    provenance.partial_year
from coverage
cross join provenance
where
    (coverage.reporter_count * 2 < coverage.most_reporters)
    != (coverage.year is not distinct from provenance.partial_year)
{% endtest %}
