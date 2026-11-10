-- Providers and recipients both carry DAC codes. The DFO holds a provider as a
-- Country or an Organization (multilaterals, funds, private foundations) and a
-- recipient as a Country or as an unallocated bucket, all under the dacCode property.
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
    spending.family,
    spending.flow,
    spending.method,
    spending.attribution,
    spending.year,
    providers.dcid as financing_provider,
    recipients.dcid as financing_recipient,
    spending.bi_multi,
    spending.value,
    spending.adaptation,
    spending.mitigation,
    spending.cross_cutting,
    spending.unsplit,
    spending.no_adaptation_marker
from {{ ref("stg_climate_finance_spending") }} as spending
left join entities as providers
    on spending.provider_code = providers.dac_code
left join entities as recipients
    on spending.recipient_code = recipients.dac_code
