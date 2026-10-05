{% set constraints = {
    "nettingBasis": "indicators.netting_basis",
    "financialFlowType": "indicators.financial_flow_type",
    "financialObligationType": "indicators.financial_obligation_type",
    "fixedCapitalConsumptionBasis": "indicators.fixed_capital_consumption_basis",
    "dacTypeOfFinance": "indicators.dac_type_of_finance",
    "dac1AidType": "indicators.dac1_aid_type",
} %}

with aid_types as (
    select
        code,
        dcid,
        {{ strip_label_parenthetical("label") }} as aid_type_label
    from {{ ref("code_mapping") }}
    where list = 'dcid:DAC1AidTypeEnum' and property = 'classificationCode'
),

indicators as (
    select
        indicator_mapping.aid_type,
        indicator_mapping.flow,
        indicator_mapping.statvar_dcid,
        indicator_mapping.population_type,
        indicator_mapping.measured_property,
        indicator_mapping.stat_type,
        indicator_mapping.measurement_qualifier,
        indicator_mapping.financial_flow_type,
        indicator_mapping.financial_obligation_type,
        indicator_mapping.netting_basis,
        indicator_mapping.fixed_capital_consumption_basis,
        indicator_mapping.dac_type_of_finance,
        aid_types.dcid as dac1_aid_type,
        indicator_mapping.denominator_aid_type,
        indicator_mapping.unit,
        indicator_mapping.entity_property,
        stat_types.dcid_segment as stat_type_dcid_segment,
        upper(left(aid_types.aid_type_label, 1))
        || substr(aid_types.aid_type_label, 2)
        || ' ('
        || case indicator_mapping.flow
            when '1120' then 'gross disbursements'
            when '1121' then 'gross disbursements, grants'
            when '1122' then 'gross disbursements, non-grants'
            when '1140' then 'net disbursements'
            when '1150' then 'commitments'
            when '1151' then 'commitments, grants'
            when '1152' then 'commitments, non-grants'
            when '1160' then 'grant equivalents'
        end
        || case
            when indicator_mapping.denominator_aid_type = '1'
                then ', % of GNI'
            else ''
        end
        || ')' as statvar_name
    from {{ ref("dac1_indicator_mapping") }} as indicator_mapping
    left join aid_types
        on indicator_mapping.dac1_aid_type = aid_types.code
    left join {{ ref("stat_type_mapping") }} as stat_types
        on indicator_mapping.stat_type = stat_types.stat_type
),

-- A share's denominator StatVar: the one the mapping names, or one minted
-- without a denominator, since none is itself a share.
denominators as (
    select
        indicators.aid_type,
        coalesce(
            indicators.statvar_dcid,
            {{
                mint_statvar_dcid(
                    stat_type_dcid_segment="indicators.stat_type_dcid_segment",
                    measured_property="indicators.measured_property",
                    population_type="indicators.population_type",
                    constraints=constraints,
                    measurement_qualifier="indicators.measurement_qualifier"
                )
            }}
        ) as dcid
    from indicators
),

statvars as (
    select
        indicators.aid_type,
        indicators.flow,
        -- A pair published under a StatVar that already exists defines none.
        indicators.statvar_dcid is null as defines_statvar,
        coalesce(
            indicators.statvar_dcid,
            {{
                mint_statvar_dcid(
                    stat_type_dcid_segment="indicators.stat_type_dcid_segment",
                    measured_property="indicators.measured_property",
                    population_type="indicators.population_type",
                    constraints=constraints,
                    measurement_qualifier="indicators.measurement_qualifier",
                    measurement_denominator="denominators.dcid"
                )
            }}
        ) as statvar_dcid,
        indicators.statvar_name,
        indicators.population_type,
        indicators.measured_property,
        indicators.stat_type,
        indicators.measurement_qualifier,
        {{ list_constraint_properties(constraints) }} as constraint_properties,
        indicators.financial_flow_type,
        indicators.financial_obligation_type,
        indicators.netting_basis,
        indicators.fixed_capital_consumption_basis,
        indicators.dac_type_of_finance,
        indicators.dac1_aid_type,
        denominators.dcid as measurement_denominator,
        indicators.unit,
        indicators.entity_property
    from indicators
    left join denominators
        on indicators.denominator_aid_type = denominators.aid_type
)

select
    statvars.statvar_dcid,
    statvars.defines_statvar,
    statvars.statvar_name,
    statvars.population_type,
    statvars.measured_property,
    statvars.stat_type,
    statvars.measurement_qualifier,
    statvars.constraint_properties,
    statvars.financial_flow_type,
    statvars.financial_obligation_type,
    statvars.netting_basis,
    statvars.fixed_capital_consumption_basis,
    statvars.dac_type_of_finance,
    statvars.dac1_aid_type,
    statvars.measurement_denominator,
    statvars.entity_property,
    unified.aid_type,
    unified.donor,
    unified.recipient,
    unified.year,
    statvars.unit,
    -- Millions to units: amounts to cents, population to whole persons.
    case
        when statvars.unit = 'dcid:Percent'
            then cast(unified.value as decimal(38, 6))
        when statvars.unit is null
            then cast(round(unified.value * 1000000) as decimal(38, 6))
        else cast(round(unified.value * 1000000, 2) as decimal(38, 6))
    end as value
from {{ ref("int_dac1_unified") }} as unified
left join statvars
    on
        unified.aid_type = statvars.aid_type
        and unified.flow = statvars.flow
