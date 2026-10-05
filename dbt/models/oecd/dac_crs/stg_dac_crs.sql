-- Amounts are rounded only after they are summed, to preserve accuracy.
select
    year,
    cast(donor_code as string) as donor_code,
    cast(agency_code as string) as agency_code,
    cast(recipient_code as string) as recipient_code,
    cast(category as string) as category,
    cast(finance_t as string) as finance_t,
    cast(aid_t as string) as aid_t,
    cast(purpose_code as string) as purpose_code,
    cast(usd_commitment as decimal(38, 12)) as usd_commitment,
    cast(usd_disbursement as decimal(38, 12)) as usd_disbursement,
    cast(usd_received as decimal(38, 12)) as usd_received,
    cast(usd_grant_equiv as decimal(38, 12)) as usd_grant_equiv
from {{ source("oecd_dac_crs", "raw_dac_crs") }}
-- bi_multi = 2 rows are a donor's core contributions to multilateral
-- organisations, which the multilateral then spends and reports under its own
-- provider code, so counting both counts the same money twice. The OECD
-- instructs that they be excluded; a null bi_multi is kept.
where bi_multi is distinct from 2
