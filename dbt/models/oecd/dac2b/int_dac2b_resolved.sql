with entities as (
    select
        code,
        dcid
    from {{ ref("code_mapping") }}
    where
        property = 'dacCode'
        and list in (
            'dcid:Country',
            'dcid:Organization',
            'dcid:DACGroup',
            'dcid:Grouping',
            'dcid:DACRecipientUnallocatedByCountry',
            'dcid:DACUnspecifiedParty',
            'dcid:WorldBankIncomeGroup',
            'dcid:UNCountryDevelopmentGroup'
        )
)

select
    dac2b.recipient_code,
    recipients.dcid as recipient,
    donors.dcid as donor,
    dac2b.aid_type,
    dac2b.amount_type,
    dac2b.year,
    dac2b.value
from {{ ref("stg_dac2b") }} as dac2b
left join entities as recipients
    on dac2b.recipient_code = recipients.code
left join entities as donors
    on dac2b.donor_code = donors.code
where
    -- Part II regional totals would collide with Part I's.
    dac2b.part = '1'
