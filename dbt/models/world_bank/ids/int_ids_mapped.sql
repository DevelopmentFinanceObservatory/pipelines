{#-
    Every IDS dimension is packed into the series code, so the whole mapping is a
    function of that one column.
-#}
with series as (
    select distinct
        series_code,
        series_name,
        series_code_pos_1 as dataset,
        series_code_pos_2 as measure,
        series_code_pos_3 as category,
        series_code_pos_4 as sector,
        series_code_pos_5 as suffix
    from {{ ref("int_ids_unified") }}
),

-- WLD carries no creditor, so a series with both shapes measures a total and its
-- parts. They become separate StatVars rather than one variable holding both.
variants as (
    select
        series_code,
        count(*) filter (where creditor is null) > 0 as has_total,
        count(*) filter (where creditor is not null) > 0 as has_breakdown
    from {{ ref("int_ids_unified") }}
    group by series_code
),

-- One row per variable a series yields, and which of its observations belong to
-- it. A series reported both ways measures a total and its parts, so it splits in
-- two, and only the breakdown takes a ByCreditor segment in its DCID. A series
-- reported one way is one variable, because there is nothing to contrast it with -
-- that covers both the series with no creditor dimension and the series whose
-- lender their code names, every row of which is attributed to that lender.
breakdowns as (
    select
        series_code,
        cast(null as varchar) as creditor_breakdown,
        false as has_provider
    from variants
    where has_total and has_breakdown
    union all
    -- No World Bank code names this, so it is a literal here rather than a
    -- code_mapping row keyed on its own node name.
    select
        series_code,
        'dcid:ByCreditor' as creditor_breakdown,
        true as has_provider
    from variants
    where has_total and has_breakdown
    union all
    select
        series_code,
        cast(null as varchar) as creditor_breakdown,
        has_breakdown as has_provider
    from variants
    where not (has_total and has_breakdown)
),

-- The columns below are ordered by what they mean - core properties, then the
-- constraints in the order they enter the DCID - rather than by whether resolving
-- one needed a coalesce, which is what ST06 would sort them by.
resolved as (
    select  -- noqa: ST06
        series.series_code,
        series.series_name,
        measures.population_type,
        measures.measured_property,
        measures.stat_type,
        coalesce(
            suffix_units.measurement_qualifier,
            sector_units.measurement_qualifier
        ) as measurement_qualifier,
        coalesce(measures.unit, suffix_units.unit, sector_units.unit) as unit,
        denominators.denominator_dcid,
        coalesce(
            denominators.denominator_series_code,
            case
                -- A DOD series with ZS in position 4 (e.g. DT.DOD.ALLC.ZS) is a share
                -- of total external debt stocks, which can't be mapped from a single
                -- segment.
                when series.measure = 'DOD' and series.sector = 'ZS'
                    then 'DT.DOD.DECT.CD'
                -- The currency composition series are shares of PPG debt stocks.
                when series.measure = 'CUR' then 'DT.DOD.DPPG.CD'
            end
        ) as denominator_series_code,
        obligation_types.dcid as financial_obligation_type,
        measures.fixed_capital_consumption_basis,
        coalesce(category_flows.dcid, measure_flows.dcid) as financial_flow_type,
        netting_bases.dcid as netting_basis,
        measures.net_of,
        position_types.dcid as financial_position_type,
        currencies.dcid as currency_of_denomination,
        {# The sector outranks the category here because it is the more specific
           segment: the US variant of use of IMF credit is the same measure
           excluding SDR allocations, so it is a loan rather than the debt
           instrument the DIMF category names on its own. #}
        coalesce(
            sector_resources.dcid, category_resources.dcid, measure_resources.dcid
        ) as financing_resource,
        financing_forms.dcid as financing_form,
        concessionality.dcid as ids_concessionality,
        creditor_types.dcid as ids_creditor_type,
        coalesce(category_guarantees.dcid, sector_guarantees.dcid) as guarantee_status,
        maturity_bands.dcid as maturity_band,
        imf_credit.dcid as includes_imf_credit,
        coalesce(category_debtors.dcid, sector_debtors.dcid) as ids_debtor_type,
        variability.dcid as interest_rate_variability,
        breakdowns.creditor_breakdown,
        breakdowns.has_provider,
        measures.entity_property
    from series
    inner join breakdowns on series.series_code = breakdowns.series_code
    left join {{ ref("ids_measure_mapping") }} as measures
        on series.dataset = measures.dataset and series.measure = measures.measure
    -- The unit is the rightmost segment that names one, not the last segment:
    -- the Debtor Reporting System series (e.g. BX.GRT.TECH.CD.DT) carry CD
    -- in position 4 (with a trailing DT marker behind it, that means debtor
    -- reporting system).
    left join {{ ref("ids_unit_mapping") }} as suffix_units
        on series.suffix = suffix_units.code and suffix_units.role = 'unit'
    left join {{ ref("ids_unit_mapping") }} as sector_units
        on series.sector = sector_units.code and sector_units.role = 'unit'
    left join {{ ref("ids_unit_mapping") }} as denominators
        on series.sector = denominators.code
    {{ ids_code_lookup(
        "obligation_types",
        "dcid:FinancialObligationTypeEnum",
        "wbCode",
        "series.measure"
    ) }}
    {{ ids_code_lookup(
        "measure_flows",
        "dcid:FinancialFlowTypeEnum",
        "wbCode",
        "series.measure"
    ) }}
    {{ ids_code_lookup(
        "category_flows",
        "dcid:FinancialFlowTypeEnum",
        "wbCode",
        "series.category"
    ) }}
    {{ ids_code_lookup(
        "netting_bases",
        "dcid:NettingBasisEnum",
        "wbCode",
        "series.measure"
    ) }}
    {{ ids_code_lookup(
        "position_types",
        "dcid:FinancialPositionTypeEnum",
        "wbCode",
        "series.measure"
    ) }}
    {{ ids_code_lookup(
        "currencies",
        "dcid:CurrencyUnit",
        "wbCode",
        "series.category"
    ) }}
    {{ ids_code_lookup(
        "measure_resources",
        "dcid:FinancingResourceEnum",
        "wbCode",
        "series.measure"
    ) }}
    {{ ids_code_lookup(
        "sector_resources",
        "dcid:FinancingResourceEnum",
        "wbCode",
        "series.sector"
    ) }}
    {{ ids_code_lookup(
        "category_resources",
        "dcid:FinancingResourceEnum",
        "wbCode",
        "series.category"
    ) }}
    {# The grants split by form rather than resource: the technical cooperation
       series and its complement are both unrequited transfers, and the form is
       what tells them apart. #}
    {{ ids_code_lookup(
        "financing_forms",
        "dcid:FinancingFormEnum",
        "wbCode",
        "series.category"
    ) }}
    {{ ids_code_lookup(
        "concessionality",
        "dcid:IDSConcessionalityEnum",
        "wbCode",
        "series.category"
    ) }}
    {{ ids_code_lookup(
        "creditor_types",
        "dcid:IDSCreditorTypeEnum",
        "classificationCode",
        "series.category"
    ) }}
    {{ ids_code_lookup(
        "category_guarantees",
        "dcid:GuaranteeStatusEnum",
        "wbCode",
        "series.category"
    ) }}
    {{ ids_code_lookup(
        "sector_guarantees",
        "dcid:GuaranteeStatusEnum",
        "wbCode",
        "series.sector"
    ) }}
    {{ ids_code_lookup(
        "maturity_bands",
        "dcid:MaturityBandEnum",
        "wbCode",
        "series.category"
    ) }}
    {{ ids_code_lookup(
        "imf_credit",
        "dcid:IncludesIMFCreditEnum",
        "wbCode",
        "series.category"
    ) }}
    {# The debt category and the debtor sector are disjoint code spaces reaching the
       same nodes - DECB and CB both give IDSDebtorTypeCB - so each is scoped to the
       property that publishes it rather than to the list they share. #}
    {{ ids_code_lookup(
        "category_debtors",
        "dcid:IDSDebtorTypeEnum",
        "classificationCode",
        "series.category"
    ) }}
    {{ ids_code_lookup(
        "sector_debtors",
        "dcid:IDSDebtorTypeEnum",
        "wbCode",
        "series.sector"
    ) }}
    {{ ids_code_lookup(
        "variability",
        "dcid:InterestRateVariabilityEnum",
        "wbCode",
        "series.category"
    ) }}
),

{%- set series_constraints = {
    "nettingBasis": "resolved.netting_basis",
    "financialFlowType": "resolved.financial_flow_type",
    "financialObligationType": "resolved.financial_obligation_type",
    "financialPositionType": "resolved.financial_position_type",
    "fixedCapitalConsumptionBasis": "resolved.fixed_capital_consumption_basis",
    "netOf": "resolved.net_of",
    "financingResource": "resolved.financing_resource",
    "financingForm": "resolved.financing_form",
    "currencyOfDenomination": "resolved.currency_of_denomination",
    "idsConcessionality": "resolved.ids_concessionality",
    "maturityBand": "resolved.maturity_band",
    "includesIMFCredit": "resolved.includes_imf_credit",
    "interestRateVariability": "resolved.interest_rate_variability",
    "guaranteeStatus": "resolved.guarantee_status",
    "idsCreditorType": "resolved.ids_creditor_type",
    "idsDebtorType": "resolved.ids_debtor_type",
} %}
{#- The breakdown names the DCID but is not a constraint: the two variables of a
    series are told apart by the entities their observations carry. #}
{%- set dcid_constraints = series_constraints.copy() %}
{%- do dcid_constraints.update({"creditorBreakdown": "resolved.creditor_breakdown"}) %}

-- A ratio series denominated by another series in this dataset names that series'
-- StatVar, in the same variant: a share per creditor divides by the amount owed to
-- that creditor, so its denominator carries the creditor breakdown too. None of
-- the denominators is itself a ratio, which is what keeps this one pass rather
-- than a recursion.
denominator_statvars as (
    select
        resolved.series_code,
        resolved.creditor_breakdown,
        {{
            mint_statvar_dcid(
                stat_type_dcid_segment="stat_types.dcid_segment",
                measured_property="resolved.measured_property",
                population_type="resolved.population_type",
                constraints=dcid_constraints,
                measurement_qualifier="resolved.measurement_qualifier"
            )
        }} as dcid
    from resolved
    left join {{ ref("stat_type_mapping") }} as stat_types
        on resolved.stat_type = stat_types.stat_type
),

-- One label per node. A node in two lists has a different suffix in each, which
-- stripping removes.
labels as (
    select
        dcid,
        min({{ strip_label_parenthetical("label") }}) as display_label
    from {{ ref("code_mapping") }}
    group by dcid
)

{%- set labelled_columns = [
    "financial_flow_type",
    "currency_of_denomination",
    "financing_resource",
    "financing_form",
    "ids_concessionality",
    "ids_creditor_type",
    "guarantee_status",
    "maturity_band",
    "includes_imf_credit",
    "ids_debtor_type",
    "interest_rate_variability",
] %}
{%- set labels = {} %}
{%- for column in labelled_columns %}
    {%- do labels.update({column: column ~ "_labels.display_label"}) %}
{%- endfor %}

select
    resolved.series_code,
    resolved.series_name,
    resolved.population_type,
    resolved.measured_property,
    resolved.stat_type,
    resolved.measurement_qualifier,
    resolved.unit,
    resolved.financial_obligation_type,
    resolved.fixed_capital_consumption_basis,
    resolved.financial_flow_type,
    resolved.netting_basis,
    resolved.net_of,
    resolved.financial_position_type,
    resolved.currency_of_denomination,
    resolved.financing_resource,
    resolved.financing_form,
    resolved.ids_concessionality,
    resolved.ids_creditor_type,
    resolved.guarantee_status,
    resolved.maturity_band,
    resolved.includes_imf_credit,
    resolved.ids_debtor_type,
    resolved.interest_rate_variability,
    resolved.creditor_breakdown,
    resolved.has_provider,
    resolved.entity_property,
    coalesce(
        resolved.denominator_dcid, denominator_statvars.dcid
    ) as measurement_denominator,
    {{ list_constraint_properties(series_constraints) }} as constraint_properties,
    {{
        mint_statvar_dcid(
            stat_type_dcid_segment="stat_types.dcid_segment",
            measured_property="resolved.measured_property",
            population_type="resolved.population_type",
            constraints=dcid_constraints,
            measurement_qualifier="resolved.measurement_qualifier",
            measurement_denominator="coalesce(resolved.denominator_dcid, denominator_statvars.dcid)"
        )
    }} as statvar_dcid,
    -- Series merged into one variable differ only by lender, which is not a
    -- property, so they compose the same name.
    {{
        mint_ids_statvar_name(
            population_type="resolved.population_type",
            measured_property="resolved.measured_property",
            stat_type="resolved.stat_type",
            financial_flow_type="resolved.financial_flow_type",
            financial_position_type="resolved.financial_position_type",
            financial_obligation_type="resolved.financial_obligation_type",
            netting_basis="resolved.netting_basis",
            net_of="resolved.net_of",
            financing_resource="resolved.financing_resource",
            unit="resolved.unit",
            denominator="coalesce(resolved.denominator_dcid, resolved.denominator_series_code)",
            creditor_breakdown="resolved.creditor_breakdown",
            labels=labels
        )
    }} as statvar_name
from resolved
left join {{ ref("stat_type_mapping") }} as stat_types
    on resolved.stat_type = stat_types.stat_type
left join denominator_statvars
    on
        resolved.denominator_series_code = denominator_statvars.series_code
        and resolved.creditor_breakdown
        is not distinct from denominator_statvars.creditor_breakdown
{% for column in labelled_columns %}
    left join labels as {{ column }}_labels
        on resolved.{{ column }} = {{ column }}_labels.dcid
{%- endfor %}
