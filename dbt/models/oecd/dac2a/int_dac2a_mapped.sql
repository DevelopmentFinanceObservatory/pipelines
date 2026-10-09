{% set constraints = {
    "nettingBasis": "indicators.netting_basis",
    "financialFlowType": "indicators.financial_flow_type",
    "dacTypeOfFinance": "indicators.dac_type_of_finance",
    "dac2aAidType": "indicators.dac2a_aid_type",
} %}

with aid_types as (
    select
        code,
        dcid,
        -- The bracketed measure says gross or net, and a gross total takes
        -- the aid type whose label ends ", net".
        regexp_replace(
            {{ strip_label_parenthetical("label") }}, ', (gross|net)$', ''
        ) as aid_type_label
    from {{ ref("code_mapping") }}
    where list = 'dcid:DAC2aAidTypeEnum' and property = 'classificationCode'
),

indicators as (
    select
        indicator_mapping.aid_type,
        indicator_mapping.population_type,
        indicator_mapping.measured_property,
        indicator_mapping.stat_type,
        indicator_mapping.measurement_qualifier,
        indicator_mapping.measurement_method,
        indicator_mapping.financial_flow_type,
        indicator_mapping.netting_basis,
        indicator_mapping.dac_type_of_finance,
        aid_types.dcid as dac2a_aid_type,
        indicator_mapping.measurement_denominator,
        indicator_mapping.unit,
        stat_types.dcid_segment as stat_type_dcid_segment,
        upper(left(aid_types.aid_type_label, 1))
        || substr(aid_types.aid_type_label, 2)
        || ' ('
        || case indicator_mapping.netting_basis
            when 'dcid:Gross' then 'gross disbursements'
            when 'dcid:Net' then 'net disbursements'
            else 'disbursements'
        end
        || case indicator_mapping.measurement_denominator
            when
                'dcid:PrimaryIncome_EconomicUnit_GrossOfFixedCapitalConsumption_Nominal'
                then ', % of recipient GNI'
            when 'dcid:Count_Person' then ', per person'
            else ''
        end
        || ')' as statvar_name
    from {{ ref("dac2a_indicator_mapping") }} as indicator_mapping
    left join aid_types
        on indicator_mapping.dac2a_aid_type = aid_types.code
    left join {{ ref("stat_type_mapping") }} as stat_types
        on indicator_mapping.stat_type = stat_types.stat_type
),

statvars as (
    select
        indicators.aid_type,
        {{
            mint_statvar_dcid(
                stat_type_dcid_segment="indicators.stat_type_dcid_segment",
                measured_property="indicators.measured_property",
                population_type="indicators.population_type",
                constraints=constraints,
                measurement_qualifier="indicators.measurement_qualifier",
                measurement_denominator="indicators.measurement_denominator"
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
        indicators.dac2a_aid_type,
        indicators.measurement_denominator,
        indicators.measurement_method,
        indicators.unit
    from indicators
)

select
    statvars.statvar_dcid,
    statvars.statvar_name,
    statvars.population_type,
    statvars.measured_property,
    statvars.stat_type,
    statvars.measurement_qualifier,
    statvars.constraint_properties,
    statvars.financial_flow_type,
    statvars.netting_basis,
    statvars.dac_type_of_finance,
    statvars.dac2a_aid_type,
    statvars.measurement_denominator,
    statvars.measurement_method,
    unified.aid_type,
    unified.recipient,
    unified.donor,
    unified.year,
    statvars.unit,
    -- Ratios are not in millions.
    case
        when statvars.measurement_denominator is not null
            then cast(unified.value as decimal(38, 6))
        else cast(round(unified.value * 1000000, 2) as decimal(38, 6))
    end as value
from {{ ref("int_dac2a_unified") }} as unified
left join statvars
    on unified.aid_type = statvars.aid_type
