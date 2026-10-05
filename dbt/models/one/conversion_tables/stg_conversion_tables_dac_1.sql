-- Total net ODA (aid type 1010, net disbursements 1140) in the three price bases
-- the DAC conversion table is computed from. The same aid type and flow in each
-- basis, so the ratio of any two is a price or currency ratio. DAC1 publishes a
-- row with no value where a donor reports nothing in a basis, such as DAC
-- Countries, Total in national currency. Those rows carry nothing to stage.
select
    cast(donor_code as string) as donor_code,
    cast(time as integer) as year,
    cast(amounttype as string) as amount_type,
    cast(value as double) as value
from {{ source("oecd_dac_1", "raw_dac_1") }}
where
    aidtype = 1010
    and flows = 1140
    and amounttype in ('A', 'D', 'N')
    and value is not null
