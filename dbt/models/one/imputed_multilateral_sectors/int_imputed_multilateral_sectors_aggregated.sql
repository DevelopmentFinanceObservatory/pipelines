-- financingChannel is not part of this series. Values are summed after
-- entities resolve, so a channel the resolver could not match still adds
-- into the total.
select
    year,
    donor,
    recipient,
    purpose_code,
    sum(value) as value
from {{ ref("int_imputed_multilateral_sectors_resolved") }}
group by all
