-- Amounts keep full precision until the observation models round them.
select
    cast(donor_code as string) as donor_code,
    cast(aidtype as string) as aid_type,
    cast(flows as string) as flow,
    cast(amounttype as string) as amount_type,
    cast(year as integer) as year,
    cast(value as decimal(38, 12)) as value
from {{ source("oecd_dac1", "raw_dac1") }}
where value is not null
