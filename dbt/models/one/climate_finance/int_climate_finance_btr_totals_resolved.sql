-- A reporting party is a Country or an Organization under its DAC code. Mexico
-- (358), Chile (434) and Indonesia (738) are not DAC donors, but the DFO holds
-- them as countries under the same dacCode property, as recipient codes in the
-- CRS, so the join the other models use resolves them to dcid:country/MEX,
-- CHL and IDN. The currency list holds the same three numbers as the codes of
-- the Mexican peso, the Chilean peso and the rupiah, which the list filter
-- keeps out.
with entities as (
    select
        codes.dcid,
        codes.code as dac_code
    from {{ ref("code_mapping") }} as codes
    where
        codes.property = 'dacCode'
        and codes.list in ('dcid:Country', 'dcid:Organization')
)

select
    btr.party,
    providers.dcid as financing_provider,
    btr.year,
    btr.status,
    btr.funding,
    btr.face_usd_at_dac
from {{ ref("stg_climate_finance_btr_totals") }} as btr
left join entities as providers
    on btr.provider_code = providers.dac_code
