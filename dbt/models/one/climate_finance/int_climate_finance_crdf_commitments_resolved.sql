-- Providers and recipients both carry DAC codes. The DFO holds a provider as a
-- Country or an Organization (multilaterals, funds, and the 16xx private
-- foundations, as it does for the CRS) and a recipient as a Country or as an
-- unallocated bucket, all under the dacCode property.
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
    crdf.year,
    providers.dcid as financing_provider,
    recipients.dcid as financing_recipient,
    crdf.bi_multi,
    crdf.climate_usd,
    crdf.adaptation_usd,
    crdf.mitigation_usd,
    crdf.overlap_usd
from {{ ref("stg_climate_finance_crdf_commitments") }} as crdf
left join entities as providers
    on crdf.provider_code = providers.dac_code
left join entities as recipients
    on crdf.recipient_code = recipients.dac_code
