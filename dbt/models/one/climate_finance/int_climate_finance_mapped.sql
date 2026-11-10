{#-
    Each figure becomes one observation per climatePolicyObjective value. The
    four narrower values are disjoint and add up to ClimateChangePolicyObjective,
    so a project marked for both adaptation and mitigation is counted once,
    under CrossCuttingClimateChangePolicyObjective.

    The UNFCCC totals have no split by objective and publish the total alone.

    The total and the three named parts are published as given, zeros
    included, since a zero there is a measured split. The unspecified part is
    a residual, so one that rounds to 0.00 carries nothing and is left out.
    assert_climate_finance_publishes_every_part pins the row count.
-#}
{% set objectives = [
    ("value", "dcid:ClimateChangePolicyObjective", ""),
    ("adaptation_only", "dcid:AdaptationOnlyClimateChangePolicyObjective", ", adaptation only"),
    ("mitigation_only", "dcid:MitigationOnlyClimateChangePolicyObjective", ", mitigation only"),
    ("cross_cutting", "dcid:CrossCuttingClimateChangePolicyObjective", ", adaptation and mitigation"),
    ("unspecified", "dcid:UnspecifiedClimateChangePolicyObjective", ", objective unspecified"),
] %}

with
-- Each figure's measurement method, and the overlap its weighting holds in
-- both adaptation and mitigation. Under joint marker assignment a project
-- marked for both objectives counts in each, so the cross-cutting part is
-- such an overlap. Under highest-marker assignment it is in neither, and the
-- overlap is 0.
assigned as (
    select
        aggregated.*,
        methods.measurement_method,
        case
            when methods.joint_marker_assignment then aggregated.cross_cutting
            else 0
        end as overlap
    from {{ ref("int_climate_finance_aggregated") }} as aggregated
    left join {{ ref("climate_finance_methods") }} as methods
        on
            aggregated.import_name = methods.import_name
            and aggregated.family = methods.family
            and aggregated.method is not distinct from methods.method
            and aggregated.attribution is not distinct from methods.attribution
),

-- The re-split is one formula for every weighting. The overlap comes out of
-- adaptation and mitigation, and the cross-cutting part is cross_cutting
-- under either assignment rule. no_adaptation_marker is finance the package
-- could not split by objective and holds in mitigation. It moves to the
-- unspecified part with the unsplit attributed finance.
split as (
    select
        import_name,
        family,
        flow,
        year,
        financing_provider,
        financing_recipient,
        measurement_method,
        value,
        cross_cutting,
        adaptation - overlap as adaptation_only,
        mitigation - overlap - no_adaptation_marker as mitigation_only,
        unsplit + no_adaptation_marker as unspecified
    from assigned
),

objectives as (
    {%- for column, objective, label in objectives %}
        select
            import_name,
            family,
            flow,
            year,
            financing_provider,
            financing_recipient,
            measurement_method,
            '{{ objective }}' as climate_policy_objective,
            '{{ label }}' as objective_label,
            {{ column }} as value
        from split
        {%- if column == "unspecified" %}
            where import_name != 'unfccc_btr' and round(unspecified, 2) != 0
        {%- elif column != "value" %}
            where import_name != 'unfccc_btr'
        {%- endif %}
        {%- if not loop.last %}
            union all
        {%- endif %}
    {%- endfor %}
),

-- Received figures measure the amount a provider gives. Provided figures are
-- a provider's attributed share of multilateral finance, measured as
-- attributedAmount. Commitments and gross disbursements take the CRS
-- pipeline's population and constraints, so these StatVars sit beside the CRS
-- ones. The UNFCCC totals cover every recipient, so their dcids and names say
-- so, and stay apart from the per-recipient received StatVars.
dc_names as (
    select
        objectives.import_name,
        objectives.year,
        objectives.financing_provider,
        objectives.financing_recipient,
        objectives.climate_policy_objective,
        objectives.value,
        'dcid:CurrencyUSD' as unit,
        'dcid:measuredValue' as stat_type,
        'dcid:Nominal' as measurement_qualifier,
        -- measurement_method is a method node and sits outside the constraints.
        -- mint_statvar_dcid leaves it out of the dcid, so every weighting and
        -- attribution rule, and every import, is a facet of one StatVar.
        objectives.measurement_method,
        case objectives.family
            when 'received' then 'dcid:amount'
            when 'provided' then 'dcid:attributedAmount'
        end as measured_property,
        case objectives.flow
            when 'commitments' then 'dcid:FinancialObligation'
            when 'disbursements' then 'dcid:FinancialFlow'
        end as population_type,
        case objectives.flow
            when 'commitments' then 'dcid:Commitment'
        end as financial_obligation_type,
        case objectives.flow
            when 'disbursements' then 'dcid:Disbursement'
        end as financial_flow_type,
        case objectives.flow
            when 'disbursements' then 'dcid:Gross'
        end as netting_basis,
        case
            when objectives.import_name = 'unfccc_btr' then 'AllRecipients'
        end as scope_segment,
        case objectives.family
            when 'received' then 'Climate finance '
            when 'provided' then 'Attributed climate finance '
        end
        || case objectives.flow
            when 'commitments' then 'commitments'
            when 'disbursements' then 'gross disbursements'
        end
        || objectives.objective_label
        || case
            when objectives.import_name = 'unfccc_btr' then ' to all recipients'
            else ''
        end as statvar_name
    from objectives
)

{% set constraints = {
    "nettingBasis": "dc_names.netting_basis",
    "financialFlowType": "dc_names.financial_flow_type",
    "financialObligationType": "dc_names.financial_obligation_type",
    "climatePolicyObjective": "dc_names.climate_policy_objective",
} %}

select
    dc_names.import_name,
    {{
        mint_statvar_dcid(
            stat_type_dcid_segment="stat_types.dcid_segment",
            measured_property="dc_names.measured_property",
            population_type="dc_names.population_type",
            constraints=constraints,
            measurement_qualifier="dc_names.measurement_qualifier",
            scope_segment="dc_names.scope_segment"
        )
    }} as statvar_dcid,
    dc_names.statvar_name,
    dc_names.population_type,
    dc_names.measured_property,
    dc_names.stat_type,
    {{ list_constraint_properties(constraints) }} as constraint_properties,
    dc_names.netting_basis,
    dc_names.financial_flow_type,
    dc_names.financial_obligation_type,
    dc_names.climate_policy_objective,
    dc_names.measurement_qualifier,
    dc_names.measurement_method,
    dc_names.financing_provider,
    dc_names.financing_recipient,
    dc_names.unit,
    dc_names.year,
    dc_names.value
from dc_names
left join {{ ref("stat_type_mapping") }} as stat_types
    on dc_names.stat_type = stat_types.stat_type
