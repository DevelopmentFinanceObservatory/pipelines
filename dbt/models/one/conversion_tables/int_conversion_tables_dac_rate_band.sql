-- Each currency-year both sources report, with the DAC1 rate over the WEO rate.
-- The two are independent. DAC1 divides national-currency ODA by US-dollar ODA,
-- and WEO divides national-currency GDP by US-dollar GDP. They differ by
-- averaging method and fiscal years by far less than a power of ten, so a ratio
-- outside a factor of two means one side read its amounts at the wrong scale, as
-- the CRS once loaded yen amounts a thousand times too small. Projected WEO years
-- are left out. The check guards DAC1's rates, so it gates the DAC export only.
select
    dac.currency,
    dac.year,
    dac.rate as dac_rate,
    imf.rate as imf_rate,
    dac.rate / imf.rate as rate_ratio
from {{ ref("int_conversion_tables_dac_rates") }} as dac
inner join {{ ref("int_conversion_tables_imf_rates") }} as imf
    on
        dac.currency = imf.currency
        and dac.year = imf.year
where not imf.is_estimate
