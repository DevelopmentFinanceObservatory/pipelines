{#-
    Each import's published total for a year matches its staged rows to within
    half a cent per observation, the most that rounding each observation to
    cents can lose. A cast that rounds each row before the aggregation sums
    them loses up to half a cent per summed row instead, and a row dropped on
    the way loses all of it, which this fails on. Only the
    ClimateChangePolicyObjective observations are summed, since the parts add
    up to them.

    An import and year present on only one side fails too.
-#}
with source_totals as (
    select
        import_name,
        year,
        sum(value) as total
    from (
        select
            'one_climate_finance' as import_name,
            year,
            value
        from {{ ref("stg_climate_finance_spending") }}
        union all
        select
            'oecd_crdf' as import_name,
            year,
            climate_usd as value
        from {{ ref("stg_climate_finance_crdf_commitments") }}
        union all
        select
            'unfccc_btr' as import_name,
            year,
            face_usd_at_dac as value
        from {{ ref("stg_climate_finance_btr_totals") }}
    )
    group by import_name, year
),

observations as (
    {%- for import_name in ["one_climate_finance", "oecd_crdf", "unfccc_btr"] %}
        select
            '{{ import_name }}' as import_name,
            observations.date as year,
            observations.value
        from {{ ref("dc_" ~ import_name ~ "_observations") }} as observations
        inner join {{ ref("dc_" ~ import_name ~ "_statvars") }} as statvars
            on observations.variable = statvars.dcid
        where statvars.climate_policy_objective = 'dcid:ClimateChangePolicyObjective'
        {%- if not loop.last %}
            union all
        {%- endif %}
    {%- endfor %}
),

published_totals as (
    select
        import_name,
        year,
        sum(value) as total,
        count(*) as observation_count
    from observations
    group by import_name, year
)

select
    source_totals.total as source_total,
    published_totals.total as published_total,
    coalesce(source_totals.import_name, published_totals.import_name) as import_name,
    coalesce(source_totals.year, published_totals.year) as year
from source_totals
full outer join published_totals
    on
        source_totals.import_name = published_totals.import_name
        and source_totals.year = published_totals.year
where
    source_totals.total is null
    or published_totals.total is null
    or abs(source_totals.total - published_totals.total)
    > published_totals.observation_count * 0.005
