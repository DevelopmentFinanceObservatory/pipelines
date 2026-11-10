{#-
    The four narrower climate change policy objectives are disjoint, so for
    every figure the parts sum to the total, under every measurement method. A
    total with no parts, or parts with no total, fails too. An unspecified part
    left out for rounding to 0.00 is under half a cent, inside the tolerance.

    The re-split subtracts the overlap from adaptation and mitigation only under
    joint marker assignment, where the source holds the cross-cutting part in
    both. Subtracting it under the other method, or not under this one, moves
    the parts' sum off the total by the cross-cutting part, so this fails in
    either direction, for negative figures as for positive ones. The UNFCCC
    totals have no parts.
-#}
with observations as (
    select
        import_name,
        measured_property,
        population_type,
        financing_provider,
        financing_recipient,
        year,
        measurement_method,
        value,
        climate_policy_objective = 'dcid:ClimateChangePolicyObjective' as is_total
    from {{ ref("int_climate_finance_mapped") }}
    where import_name != 'unfccc_btr'
),

totals as (
    select * exclude (is_total)
    from observations
    where is_total
),

parts as (
    select
        import_name,
        measured_property,
        population_type,
        financing_provider,
        financing_recipient,
        year,
        measurement_method,
        sum(value) as value
    from observations
    where not is_total
    group by all
)

select
    totals.value as total,
    parts.value as parts,
    coalesce(totals.import_name, parts.import_name) as import_name,
    coalesce(totals.measured_property, parts.measured_property) as measured_property,
    coalesce(totals.population_type, parts.population_type) as population_type,
    coalesce(totals.financing_provider, parts.financing_provider) as financing_provider,
    coalesce(totals.financing_recipient, parts.financing_recipient) as financing_recipient,
    coalesce(totals.year, parts.year) as year,
    coalesce(totals.measurement_method, parts.measurement_method) as measurement_method
from totals
full outer join parts
    on
        totals.import_name = parts.import_name
        and totals.measured_property = parts.measured_property
        and totals.population_type = parts.population_type
        and totals.financing_provider = parts.financing_provider
        and totals.financing_recipient = parts.financing_recipient
        and totals.year = parts.year
        and totals.measurement_method = parts.measurement_method
where
    totals.value is null
    or parts.value is null
    or abs(totals.value - parts.value) > 0.01
