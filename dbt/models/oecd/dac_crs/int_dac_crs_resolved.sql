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
    crs.year,
    donors.dcid as donor,
    recipients.dcid as recipient,
    crs.category,
    crs.finance_t,
    crs.aid_t,
    crs.purpose_code,
    crs.usd_commitment,
    crs.usd_disbursement,
    crs.usd_received,
    crs.usd_grant_equiv
from {{ ref("stg_dac_crs") }} as crs
left join entities as donors
    on crs.donor_code = donors.dac_code
left join entities as recipients
    on crs.recipient_code = recipients.dac_code
