-- Only committed and disbursed rows with a face value carry a figure to publish.
-- A row with no status has no flow to publish under, and a row with only a grant
-- equivalent has no face value. The source tests check that a party, year and
-- status hold a face value on every row or on none, so the rows dropped here are
-- whole groups and no published total is partial. The face value is the party's
-- own-currency amount converted at the OECD DAC exchange rate. Amounts keep full
-- precision and are rounded only in the observation model.
select
    party,
    cast(provider_code as string) as provider_code,
    cast(year as integer) as year,
    status,
    funding,
    cast(face_usd_at_dac as decimal(38, 12)) as face_usd_at_dac
from {{ source("one_climate_finance", "raw_climate_finance_btr_totals") }}
where
    status in ('committed', 'disbursed')
    and face_usd_at_dac is not null
