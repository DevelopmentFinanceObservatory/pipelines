with entities as (
    select
        codes.dcid,
        codes.code as dac_code
    from {{ ref("code_mapping") }} as codes
    where
        codes.property = 'dacCode'
        and codes.list in (
            'dcid:Country',
            'dcid:Organization',
            'dcid:DACRecipientUnallocatedByCountry'
        )
)

select
    imputed.year,
    donors.dcid as donor,
    recipients.dcid as recipient,
    imputed.purpose_code,
    imputed.channel_code,
    imputed.value
from {{ ref("stg_imputed_multilateral_sectors") }} as imputed
left join entities as donors
    on imputed.donor_code = donors.dac_code
left join entities as recipients
    on imputed.recipient_code = recipients.dac_code
