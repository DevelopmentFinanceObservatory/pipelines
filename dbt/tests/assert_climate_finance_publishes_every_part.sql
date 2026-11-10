{#-
    Every ONE Data and OECD figure publishes its total and its adaptation-only,
    mitigation-only and cross-cutting parts, zeros included, and every UNFCCC
    figure its total alone. So each import has one observation per figure under
    each of those objectives, and none under any other. A filter that drops
    zero amounts anywhere between the aggregation and the mapping fails here,
    which the sum tests miss, since a zero adds nothing to a sum.

    The unspecified part is published only where it holds a cent, so an
    unspecified observation that rounds to 0.00, or one for the UNFCCC, fails.
    assert_climate_finance_parts_sum_to_total fails on a missing unspecified
    part that holds more.
-#}
with figures as (
    select
        import_name,
        count(*) as figure_count
    from {{ ref("int_climate_finance_aggregated") }}
    group by import_name
),

objectives as (
    select climate_policy_objective
    from (
        values
        ('dcid:ClimateChangePolicyObjective'),
        ('dcid:AdaptationOnlyClimateChangePolicyObjective'),
        ('dcid:MitigationOnlyClimateChangePolicyObjective'),
        ('dcid:CrossCuttingClimateChangePolicyObjective')
    ) as named (climate_policy_objective)
),

expected as (
    select
        figures.import_name,
        objectives.climate_policy_objective,
        figures.figure_count as row_count
    from figures
    cross join objectives
    where
        figures.import_name != 'unfccc_btr'
        or objectives.climate_policy_objective = 'dcid:ClimateChangePolicyObjective'
),

published as (
    select
        import_name,
        climate_policy_objective,
        count(*) as row_count
    from {{ ref("int_climate_finance_mapped") }}
    where climate_policy_objective != 'dcid:UnspecifiedClimateChangePolicyObjective'
    group by import_name, climate_policy_objective
)

select
    coalesce(expected.import_name, published.import_name) as import_name,
    coalesce(
        expected.climate_policy_objective, published.climate_policy_objective
    ) as climate_policy_objective,
    expected.row_count as expected_row_count,
    published.row_count as published_row_count,
    null as value
from expected
full outer join published
    on
        expected.import_name = published.import_name
        and expected.climate_policy_objective = published.climate_policy_objective
where expected.row_count is distinct from published.row_count

union all

select
    import_name,
    climate_policy_objective,
    null as expected_row_count,
    null as published_row_count,
    value
from {{ ref("int_climate_finance_mapped") }}
where
    climate_policy_objective = 'dcid:UnspecifiedClimateChangePolicyObjective'
    and (import_name = 'unfccc_btr' or round(value, 2) = 0)
