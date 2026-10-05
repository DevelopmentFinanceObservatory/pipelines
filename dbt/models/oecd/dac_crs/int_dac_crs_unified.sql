with
with_net_disbursements as (
    select
        crs.*,
        -- A row reporting a repayment but no disbursement is a negative net
        -- disbursement, so it is kept whenever either side is present.
        case
            when coalesce(crs.usd_disbursement, crs.usd_received) is null
                then null
            else
                coalesce(crs.usd_disbursement, 0)
                - coalesce(crs.usd_received, 0)
        end as usd_net_disbursement
    from {{ ref("int_dac_crs_resolved") }} as crs
),

measure_values as (
    select
        year,
        donor,
        recipient,
        category,
        finance_t,
        aid_t,
        purpose_code,
        measure,
        value
    from with_net_disbursements
    unpivot (value for measure in (
        usd_commitment,
        usd_disbursement,
        usd_net_disbursement,
        usd_grant_equiv
    ))
    where value is not null
),

measure_dimensions as (
    select dimensions.*
    from (
        values
        ('usd_commitment', 'commitment', 'gross', 'face_value'),
        ('usd_disbursement', 'disbursement', 'gross', 'face_value'),
        ('usd_net_disbursement', 'disbursement', 'net', 'face_value'),
        ('usd_grant_equiv', 'disbursement', null, 'grant_equivalent')
    ) as dimensions (
        measure, financing_event_type, netting_basis, valuation_basis
    )
)

select
    measure_values.year,
    measure_values.donor,
    measure_values.recipient,
    measure_values.category,
    measure_values.finance_t,
    measure_values.aid_t,
    measure_values.purpose_code,
    dimensions.financing_event_type,
    dimensions.netting_basis,
    dimensions.valuation_basis,
    cast(sum(measure_values.value) as decimal(38, 12)) as value
from measure_values
left join measure_dimensions as dimensions
    on measure_values.measure = dimensions.measure
group by all
