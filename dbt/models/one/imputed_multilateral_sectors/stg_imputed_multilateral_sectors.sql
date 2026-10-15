-- A fixed_purpose proxy row (e.g. UN peacekeeping core contributions under purpose
-- code 15230) carries a null recipient_code, since a fixed purpose code carries no
-- recipient of its own; map it to DAC recipient code 9998, "Bilateral,
-- unallocated/unspecified" (dcid:DACBilateralUnallocated), the code the CRS uses
-- for spending no single recipient can be named for. oda-data marks it as a
-- `proxy` row with a null share_channel_code; a parent_fund proxy row carries
-- the proxy channel there. An unallocated row carries both a null recipient_code
-- and a null purpose_code (no share could be found for its channel and year at
-- all); map its purpose_code the same way, to CRS purpose code 998,
-- "Unallocated/unspecified" (dcid:CRSPurposeCode998). Its share of that
-- channel-year's core contribution is also reported as a coverage figure in the
-- raw table asset's materialization metadata. A null anywhere else is malformed
-- upstream data and stays null, so the not_null tests fail on it.
-- value is rounded only after it is summed, to preserve accuracy.
select
    cast(donor_code as string) as donor_code,
    cast(channel_code as string) as channel_code,
    cast(year as integer) as year,
    cast(value as decimal(38, 12)) as value,
    case
        when
            allocation_status = 'unallocated'
            or (allocation_status = 'proxy' and share_channel_code is null)
            then coalesce(cast(recipient_code as string), '9998')
        else cast(recipient_code as string)
    end as recipient_code,
    case
        when allocation_status = 'unallocated'
            then coalesce(cast(purpose_code as string), '998')
        else cast(purpose_code as string)
    end as purpose_code
from {{ source("one_imputed_multilateral_sectors", "raw_imputed_multilateral_sectors") }}
where
    allocation_status in ('imputed', 'stale_share', 'proxy', 'unallocated')
    and {{ exclude_incomplete_years("one_imputed_multilateral_sectors") }}
