-- Total net ODA (aid type 1010, net disbursements 1140) in the three price bases
-- the DAC conversion table is computed from. The same aid type and flow in each
-- basis, so the ratio of any two is a price or currency ratio.
-- DAC Countries, Total has no node, so it takes the dcid the DAC recipe falls
-- back to.
with donors as (
    select
        code as dac_code,
        dcid
    from {{ ref("code_mapping") }}
    where
        property = 'dacCode'
        and list in ('dcid:Country', 'dcid:Organization')
)

select
    dac1.year,
    dac1.amount_type,
    cast(dac1.value as double) as value,
    case
        when dac1.donor_code = '20001' then 'dcid:DACCountriesTotal'
        else donors.dcid
    end as entity
from {{ ref("stg_dac1") }} as dac1
left join donors
    on dac1.donor_code = donors.dac_code
where
    dac1.aid_type = '1010'
    and dac1.flow = '1140'
    and {{ is_dac1_donor("dac1.donor_code") }}
