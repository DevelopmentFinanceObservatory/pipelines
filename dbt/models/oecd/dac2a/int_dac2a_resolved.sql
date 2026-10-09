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
    dac2a.recipient_code,
    recipients.dcid as recipient,
    donors.dcid as donor,
    dac2a.aid_type,
    dac2a.amount_type,
    dac2a.year,
    dac2a.value
from {{ ref("stg_dac2a") }} as dac2a
left join entities as recipients
    on dac2a.recipient_code = recipients.code
left join entities as donors
    on dac2a.donor_code = donors.code
where
    -- Part II flows are official aid, not ODA, under the same aid types.
    dac2a.part = '1'
