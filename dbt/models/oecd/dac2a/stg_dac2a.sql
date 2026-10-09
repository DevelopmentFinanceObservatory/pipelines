select
    cast(recipient_code as string) as recipient_code,
    cast(donor_code as string) as donor_code,
    cast(part_code as string) as part,
    cast(aidtype as string) as aid_type,
    cast(datatype as string) as amount_type,
    cast(year as integer) as year,
    cast(value as decimal(38, 12)) as value
from {{ source("oecd_dac2a", "raw_dac2a") }}
where value is not null
