select
    donor,
    aid_type,
    flow,
    year,
    value,
    -- Add recipient for West Bank and Gaza Strip aid types.
    case
        when aid_type in ('8863', '8864') then 'dcid:country/PSE'
    end as recipient
from {{ ref("int_dac1_resolved") }}
where
    -- Current US dollars only.
    amount_type = 'A'
    -- Drop amounts received.
    and flow != '1130'
    -- Repeated by 11010, 11015 and 12000, which cover more years.
    and not (aid_type in ('1010', '1015', '2000') and flow = '1160')
    -- GNI and population, kept once under net disbursements.
    and not (aid_type in ('1', '4') and flow != '1140')
    -- Repeats aid type 11002, rounded.
    and not (aid_type = '3' and flow = '1160')
