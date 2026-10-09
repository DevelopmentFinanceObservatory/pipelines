-- Donor codes that share a node repeat each other, such as 915 and 20015 for
-- the Asian Development Bank, so one copy is kept.
with donor_repeats_removed as (
    select distinct
        recipient_code,
        recipient,
        donor,
        aid_type,
        year,
        value
    from {{ ref("int_dac2b_resolved") }}
    where amount_type = 'A'
)

-- Recipient codes that share a node are different flows to it, such as IDA
-- (905) and IDA-MDRI (904), so they are added.
select
    recipient,
    donor,
    aid_type,
    year,
    sum(value) as value
from donor_repeats_removed
group by recipient, donor, aid_type, year
