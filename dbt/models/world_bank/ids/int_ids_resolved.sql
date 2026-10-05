with
-- The debtor and creditor code spaces are disjoint, but their entities overlap:
-- most country nodes are both a debtor and a creditor, so one node holds a code
-- from each space. Each space therefore carries its own property, and a lookup
-- is scoped to that property rather than to the lists it spans.
debtors as (
    select
        code as wb_economy_code,
        dcid
    from {{ ref("code_mapping") }}
    where property = 'wbEconomyCode'
),

creditors as (
    select
        code as wb_counterpart_code,
        dcid
    from {{ ref("code_mapping") }}
    where property = 'wbCounterpartCode'
),

-- Nine segment codes name a single lender in the third segment instead of the
-- counterpart column: DT.DOD.MIBR.CD is debt owed to the IBRD. That is the same
-- fact in a different place, so it is normalised into the creditor here rather
-- than carried on to become a second way of saying who lent the money. RDBC,
-- RDBN and MOTH carry a lender in that segment too, but name a class rather than
-- an institution, so they resolve nothing here and say it through
-- idsCreditorType instead - which is what the list scope below enforces.
segment_creditors as (
    select
        code as wb_code,
        dcid
    from {{ ref("code_mapping") }}
    where list = 'dcid:Organization' and property = 'wbCode'
),

normalised as (
    select
        debtors.dcid as debtor,
        creditors.dcid as counterpart_creditor,
        segment_creditors.dcid as segment_creditor,
        ids.counterpart_code,
        ids.country_code,
        ids.series_code,
        ids.series_name,
        ids.year,
        ids.value,
        -- The segment lender backs up the WLD total, which names no counterpart
        -- of its own. It is not a fallback for a counterpart that fails to
        -- resolve: attributing a new code to the lender the series names would
        -- silently publish a wrong provider and slip past the unresolved-entity
        -- guard below, so a non-WLD counterpart that resolves nothing stays null
        -- and fails that guard.
        coalesce(
            creditors.dcid,
            case when ids.counterpart_code = 'WLD' then segment_creditors.dcid end
        ) as creditor
    from {{ ref("int_ids_pruned") }} as ids
    left join debtors
        on ids.country_code = debtors.wb_economy_code
    left join creditors
        on ids.counterpart_code = creditors.wb_counterpart_code
    left join segment_creditors
        on split_part(ids.series_code, '.', 3) = segment_creditors.wb_code
),

-- The cells where the lender a series code names is already itemised against a
-- counterpart. Built straight from the source rather than from `normalised`, so
-- the join chain over 147 million rows is evaluated once rather than twice, and
-- inner joined so only the few rows that name their own lender are scanned.
restated_totals as (
    select distinct
        ids.series_code,
        ids.country_code,
        ids.year
    from {{ ref("int_ids_pruned") }} as ids
    inner join segment_creditors
        on split_part(ids.series_code, '.', 3) = segment_creditors.wb_code
    inner join creditors
        on
            ids.counterpart_code = creditors.wb_counterpart_code
            and segment_creditors.dcid = creditors.dcid
)

select
    normalised.debtor,
    normalised.creditor,
    -- 'counterpart_code' is retained so a null creditor is interpretable
    -- downstream: WLD is deliberately unmodelled, so its rows carry no creditor
    -- DCID and would otherwise be indistinguishable from a code that failed to
    -- resolve.
    normalised.counterpart_code,
    normalised.series_code,
    -- Carried from here to the Data Commons layer, which names each StatVar after
    -- the series it measures. It is an attribute of the series rather than of the
    -- observation, so it repeats across every row of a series.
    normalised.series_name,
    normalised.year,
    normalised.value
from normalised
-- A WLD row on one of those eight series is the total across a dimension holding
-- one lender, so it restates the row that names that lender and is dropped. Where
-- the source published no such row - all of NIFC, and some of IDAG - the WLD row
-- is the only record, so it is kept and carries the lender the series names.
-- Joined on the keys alone: a constant in the ON clause costs the planner its hash
-- join, and probing 147 million rows any other way spills to disk.
left join restated_totals
    on
        normalised.series_code = restated_totals.series_code
        and normalised.country_code = restated_totals.country_code
        and normalised.year = restated_totals.year
where
    restated_totals.series_code is null
    or normalised.counterpart_code <> 'WLD'
