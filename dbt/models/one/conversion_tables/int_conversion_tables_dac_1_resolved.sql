-- DAC Countries, Total has no node in the ontology's code mapping, so its dcid is
-- the literal the DAC recipe names as its fallback deflator entity.
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
    dac_1.year,
    dac_1.amount_type,
    dac_1.value,
    case
        when dac_1.donor_code = '20001' then 'dcid:DACCountriesTotal'
        else donors.dcid
    end as entity
from {{ ref("stg_conversion_tables_dac_1") }} as dac_1
left join donors
    on dac_1.donor_code = donors.dac_code
where {{ is_dac_1_donor("dac_1.donor_code") }}
