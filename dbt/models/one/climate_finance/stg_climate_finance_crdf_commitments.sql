-- Every row is kept, private grants, memorandum rows and repeats included, as the
-- package ships the OECD's CRDF.
--
-- 998 is the package's code for developing countries, unspecified, which the DFO
-- holds as DAC recipient 9998, "Bilateral, unallocated/unspecified"
-- (dcid:DACBilateralUnallocated). Amounts keep full precision and are rounded
-- only in the observation model.
select
    cast(year as integer) as year,
    cast(provider_code as string) as provider_code,
    cast(bi_multi as integer) as bi_multi,
    cast(climate_usd as decimal(38, 12)) as climate_usd,
    cast(adaptation_usd as decimal(38, 12)) as adaptation_usd,
    cast(mitigation_usd as decimal(38, 12)) as mitigation_usd,
    cast(overlap_usd as decimal(38, 12)) as overlap_usd,
    case
        when recipient_code = 998 then '9998'
        else cast(recipient_code as string)
    end as recipient_code
from {{ source("one_climate_finance", "raw_climate_finance_crdf_commitments") }}
