select areas.dcid
from {{ ref("code_mapping") }} as areas
inner join {{ ref("weo_currency_mapping") }} as area_currencies
    on areas.code = area_currencies.imf_area_code
where
    areas.property = 'imfEconomyCode'
    and area_currencies.iso4217_code = 'USD'
