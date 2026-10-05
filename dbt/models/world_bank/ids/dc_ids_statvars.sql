{#-
    One row per StatVar, not per series. Four series describe two variables: the
    World Bank reports net flows from the IBRD and from the IMF's non-concessional
    lending separately, but once the lender sits on the observation those are one
    measure, so they mint one DCID and are grouped here.
-#}
-- Grouped by every column except the series code and name, which differ within a
-- merge. Columns are ordered by meaning, not by whether they need an aggregate.
select  -- noqa: ST06
    statvar_dcid as dcid,
    statvar_name as name,
    -- The World Bank name of each source series, in code order.
    string_agg(series_name, '; ' order by series_code) as description,
    population_type,
    measured_property,
    stat_type,
    measurement_qualifier,
    measurement_denominator,
    constraint_properties,
    financial_obligation_type,
    economic_activity_type,
    financial_flow_type,
    netting_basis,
    net_of,
    financial_position_type,
    currency_of_denomination,
    financing_resource,
    financing_form,
    ids_concessionality,
    ids_creditor_type,
    guarantee_status,
    maturity_band,
    includes_imf_credit,
    ids_debtor_type,
    interest_rate_variability,
    has_provider,
    -- Every series the variable was minted from, one quoted MCF value each, so the
    -- link back to the source survives a merge.
    '"' || string_agg(series_code, '","' order by series_code) || '"'
        as source_indicator_code,
    -- A variable whose observations carry no provider is measured for the debtor
    -- alone, so it declares one entity; the rest name both parties.
    case
        when has_provider then 'financingProvider,financingRecipient'
        else 'financingRecipient'
    end as observation_properties
from {{ ref("int_ids_mapped") }}
group by all
