-- A DAC code also names a currency, so only donor lists are joined.
with donors as (
    select
        code,
        dcid
    from {{ ref("code_mapping") }}
    where
        property = 'dacCode'
        and list in (
            'dcid:Country', 'dcid:Organization', 'dcid:DACGroup', 'dcid:Grouping'
        )
)

select
    donors.dcid as donor,
    dac1.aid_type,
    dac1.flow,
    dac1.amount_type,
    dac1.year,
    dac1.value
from {{ ref("stg_dac1") }} as dac1
left join donors
    on dac1.donor_code = donors.code
