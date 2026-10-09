{% set constraints = {
    "nettingBasis": "indicators.netting_basis",
    "financialFlowType": "indicators.financial_flow_type",
    "dacTypeOfFinance": "indicators.dac_type_of_finance",
    "dac2bAidType": "indicators.dac2b_aid_type",
} %}

with aid_types as (
    select
        code,
        dcid,
        -- The bracketed measure says gross or net, so a label must not.
        regexp_replace(
            {{ strip_label_parenthetical("label") }}, ', (gross|net)$', ''
        ) as aid_type_label
    from {{ ref("code_mapping") }}
    where list = 'dcid:DAC2bAidTypeEnum' and property = 'classificationCode'
),

indicators as (
    select
        indicator_mapping.aid_type,
        indicator_mapping.population_type,
        indicator_mapping.measured_property,
        indicator_mapping.stat_type,
        indicator_mapping.measurement_qualifier,
        indicator_mapping.financial_flow_type,
        indicator_mapping.netting_basis,
        indicator_mapping.dac_type_of_finance,
        aid_types.dcid as dac2b_aid_type,
        indicator_mapping.unit,
        stat_types.dcid_segment as stat_type_dcid_segment,
        upper(left(aid_types.aid_type_label, 1))
        || substr(aid_types.aid_type_label, 2)
        || ' ('
        || case indicator_mapping.netting_basis
            when 'dcid:Gross' then 'gross disbursements'
            when 'dcid:Net' then 'net disbursements'
        end
        || ')' as statvar_name
    from {{ ref("dac2b_indicator_mapping") }} as indicator_mapping
    left join aid_types
        on indicator_mapping.dac2b_aid_type = aid_types.code
    left join {{ ref("stat_type_mapping") }} as stat_types
        on indicator_mapping.stat_type = stat_types.stat_type
)

select
    {{
        mint_statvar_dcid(
            stat_type_dcid_segment="indicators.stat_type_dcid_segment",
            measured_property="indicators.measured_property",
            population_type="indicators.population_type",
            constraints=constraints,
            measurement_qualifier="indicators.measurement_qualifier"
        )
    }} as statvar_dcid,
    indicators.statvar_name,
    indicators.population_type,
    indicators.measured_property,
    indicators.stat_type,
    indicators.measurement_qualifier,
    {{ list_constraint_properties(constraints) }} as constraint_properties,
    indicators.financial_flow_type,
    indicators.netting_basis,
    indicators.dac_type_of_finance,
    indicators.dac2b_aid_type,
    unified.aid_type,
    unified.recipient,
    unified.donor,
    unified.year,
    indicators.unit,
    cast(round(unified.value * 1000000, 2) as decimal(38, 6)) as value
from {{ ref("int_dac2b_unified") }} as unified
left join indicators
    on unified.aid_type = indicators.aid_type
