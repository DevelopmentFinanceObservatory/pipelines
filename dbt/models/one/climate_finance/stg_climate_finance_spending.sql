-- Provided rows with a Bi_Multi code other than 2 repeat the figures the received
-- rows hold, so only the attributed rows (core contributions, Bi_Multi 2) are
-- kept. Provided rows with no provider are attributed value under the outflows
-- rule that no developed provider takes, and are dropped too. The source's
-- only_attributed_outflows_lack_a_provider test shows no other row lacks one.
--
-- 998 is the package's code for developing countries, unspecified, which the DFO
-- holds as DAC recipient 9998, "Bilateral, unallocated/unspecified"
-- (dcid:DACBilateralUnallocated). A provided core contribution whose
-- multilateral had no outflows to spread it over has no recipient, so it maps to
-- 9998 too. A null recipient on any other row is malformed and stays null, so
-- the not_null test fails on it.
--
-- Rows keep their Bi_Multi code, and the aggregation sums over it. Amounts keep full
-- precision and are rounded only in the observation model.
select
    family,
    flow,
    method,
    attribution,
    cast(year as integer) as year,
    cast(provider_code as string) as provider_code,
    cast(bi_multi as integer) as bi_multi,
    cast(value as decimal(38, 12)) as value,
    cast(adaptation as decimal(38, 12)) as adaptation,
    cast(mitigation as decimal(38, 12)) as mitigation,
    cast(cross_cutting as decimal(38, 12)) as cross_cutting,
    cast(unsplit as decimal(38, 12)) as unsplit,
    cast(no_adaptation_marker as decimal(38, 12)) as no_adaptation_marker,
    case
        when recipient_code = 998 or (recipient_code is null and family = 'provided')
            then '9998'
        else cast(recipient_code as string)
    end as recipient_code
from {{ source("one_climate_finance", "raw_climate_finance_spending") }}
where
    (family = 'received' or bi_multi = 2)
    and provider_code is not null
