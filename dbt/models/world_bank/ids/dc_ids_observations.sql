-- The IDS reports whole units of currency, not millions as the CRS does, so the
-- value is carried through unscaled. decimal(38,5) is kept from stg_ids because the
-- same column holds both trillion-dollar stocks and sub-percent ratios.
with last_reported as (
    -- The debt stock is reported and never projected, so its last year is the last
    -- year of reported data.
    select max(year) as year
    from {{ ref("int_ids_unified") }}
    where series_code = 'DT.DOD.DECT.CD'
)

select
    mapped.statvar_dcid as variable,
    unified.year as date,
    unified.creditor as financing_provider,
    unified.debtor as financing_recipient,
    unified.value,
    mapped.unit,
    case
        when unified.year > last_reported.year then 'dcid:Estimated'
    end as measurement_method
from {{ ref("int_ids_unified") }} as unified
cross join last_reported
inner join {{ ref("int_ids_mapped") }} as mapped
    on
        unified.series_code = mapped.series_code
        -- A row belongs to the variable whose observations carry a provider where
        -- it carries one. For a series reported both ways that separates the WLD
        -- total from its itemised parts; for a series reported one way there is a
        -- single variable and every row lands on it.
        and (unified.creditor is not null) = mapped.has_provider
