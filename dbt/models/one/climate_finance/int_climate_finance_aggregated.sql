-- The three imports at their published grain, in one shape so the objective
-- split in int_climate_finance_mapped is one formula for all of them.
--
-- ONE Data's figures are summed over the Bi_Multi code. The OECD's commitments
-- are summed over every row a year, provider and recipient share, and take the
-- OECD's full-value weighting. Their overlap_usd is the cross-cutting part,
-- held in adaptation and mitigation alike, and they have no unsplit part and
-- no part without an adaptation marker. The UNFCCC
-- totals are summed over the type of funding, carry no recipient and have no
-- split by objective. Values are summed after entities resolve, so a code the
-- resolver could not match still adds into the total.
with spending as (
    select
        'one_climate_finance' as import_name,
        family,
        flow,
        method,
        attribution,
        year,
        financing_provider,
        financing_recipient,
        cast(sum(value) as decimal(38, 12)) as value,
        cast(sum(adaptation) as decimal(38, 12)) as adaptation,
        cast(sum(mitigation) as decimal(38, 12)) as mitigation,
        cast(sum(cross_cutting) as decimal(38, 12)) as cross_cutting,
        cast(sum(unsplit) as decimal(38, 12)) as unsplit,
        cast(sum(no_adaptation_marker) as decimal(38, 12)) as no_adaptation_marker
    from {{ ref("int_climate_finance_spending_resolved") }}
    group by
        family,
        flow,
        method,
        attribution,
        year,
        financing_provider,
        financing_recipient
),

crdf as (
    select
        'oecd_crdf' as import_name,
        'received' as family,  -- noqa: RF04
        'commitments' as flow,
        'OECD' as method,  -- noqa: RF04
        cast(null as string) as attribution,
        year,
        financing_provider,
        financing_recipient,
        cast(sum(climate_usd) as decimal(38, 12)) as value,
        cast(sum(adaptation_usd) as decimal(38, 12)) as adaptation,
        cast(sum(mitigation_usd) as decimal(38, 12)) as mitigation,
        cast(sum(overlap_usd) as decimal(38, 12)) as cross_cutting,
        cast(0 as decimal(38, 12)) as unsplit,
        cast(0 as decimal(38, 12)) as no_adaptation_marker
    from {{ ref("int_climate_finance_crdf_commitments_resolved") }}
    group by
        year,
        financing_provider,
        financing_recipient
),

btr as (
    select
        year,
        financing_provider,
        'unfccc_btr' as import_name,
        'received' as family,  -- noqa: RF04
        cast(null as string) as method,  -- noqa: RF04
        cast(null as string) as attribution,
        cast(null as string) as financing_recipient,
        cast(null as decimal(38, 12)) as adaptation,
        cast(null as decimal(38, 12)) as mitigation,
        cast(null as decimal(38, 12)) as cross_cutting,
        cast(null as decimal(38, 12)) as unsplit,
        cast(null as decimal(38, 12)) as no_adaptation_marker,
        cast(sum(face_usd_at_dac) as decimal(38, 12)) as value,
        case status
            when 'committed' then 'commitments'
            when 'disbursed' then 'disbursements'
        end as flow
    from {{ ref("int_climate_finance_btr_totals_resolved") }}
    group by
        status,
        year,
        financing_provider
)

select
    import_name,
    family,
    flow,
    method,
    attribution,
    year,
    financing_provider,
    financing_recipient,
    value,
    adaptation,
    mitigation,
    cross_cutting,
    unsplit,
    no_adaptation_marker
from spending

union all

select
    import_name,
    family,
    flow,
    method,
    attribution,
    year,
    financing_provider,
    financing_recipient,
    value,
    adaptation,
    mitigation,
    cross_cutting,
    unsplit,
    no_adaptation_marker
from crdf

union all

select
    import_name,
    family,
    flow,
    method,
    attribution,
    year,
    financing_provider,
    financing_recipient,
    value,
    adaptation,
    mitigation,
    cross_cutting,
    unsplit,
    no_adaptation_marker
from btr
