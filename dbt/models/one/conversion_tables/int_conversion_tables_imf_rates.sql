-- Each currency's rate in the IMF table per year: its reference area's own rate.
-- The inner join selects the reference area's rows. Every other member's rows
-- are read only by int_conversion_tables_imf_rate_agreement.
select
    members.currency,
    members.currency_unit,
    members.year,
    members.rate,
    members.is_estimate
from {{ ref("int_conversion_tables_imf_rate_members") }} as members
inner join {{ ref("int_conversion_tables_imf_rate_references") }} as rate_references
    on
        members.currency = rate_references.currency
        and members.area = rate_references.reference_area
