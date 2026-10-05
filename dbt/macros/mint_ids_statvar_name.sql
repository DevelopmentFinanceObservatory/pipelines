{% macro mint_ids_statvar_name(
    population_type, measured_property, stat_type, financial_flow_type,
    financial_position_type, financial_obligation_type, netting_basis, net_of,
    financing_resource, unit, denominator, creditor_breakdown, labels
) %}
    {#-
        Human-readable name for an IDS StatVar, composed from its properties:
        `<measure> <debt> [owed by the <debtor>] [to <creditor>][, by creditor] (<unit>)`

        `labels` maps each constrained column to its node's label, suffix stripped.
        `denominator` is the denominator's series code, or the DCID of one Data
        Commons already defines. The unit is in the name because a ratio shares
        every other property with its numerator. An unknown unit or denominator
        gives a null name, which the not_null test catches.

        concat_ws skips a null argument and its separator, so a missing debtor or
        creditor leaves no gap.
    -#}
    {%- set is_grant = financing_resource ~ " = 'dcid:UnrequitedTransfer'" -%}
    {#- Terms and instrument that read as qualifiers, e.g. `(without IMF credit)`. -#}
    {%- set qualifiers -%}
        concat_ws(
            ', ',
            {{ labels.includes_imf_credit }},
            {{ labels.interest_rate_variability }},
            case when not {{ is_grant }} then {{ labels.financing_resource }} end,
            {{ labels.financing_form }}
        )
    {%- endset -%}
    concat_ws(
        ' ',
        -- Measure: what is counted, e.g. `Disbursements on`, `Outstanding`.
        case
            when {{ measured_property }} = 'dcid:primaryIncome'
                then 'Gross national income'
            when {{ stat_type }} = 'dcid:meanValue'
                then 'Average ' || case {{ measured_property }}
                    when 'dcid:interestRate' then 'interest rate'
                    when 'dcid:maturity' then 'maturity'
                    when 'dcid:gracePeriod' then 'grace period'
                    when 'dcid:grantElement' then 'grant element'
                end || ' on new commitments of'
            when
                {{ netting_basis }} = 'dcid:Net'
                and {{ net_of }} = 'dcid:PrincipalRepayment'
                then 'Net flows on'
            when
                {{ netting_basis }} = 'dcid:Net'
                and {{ net_of }} = 'dcid:ServicingPayment'
                then 'Net transfers on'
            when {{ financial_obligation_type }} = 'dcid:Commitment'
                then 'Commitments of'
            when {{ financial_flow_type }} = 'dcid:Disbursement' and {{ is_grant }}
                then 'Disbursements of'
            when {{ financial_flow_type }} = 'dcid:Disbursement'
                then 'Disbursements on'
            when {{ financial_flow_type }} = 'dcid:InterestPayment'
                then 'Interest payments on'
            when {{ financial_flow_type }} = 'dcid:PrincipalRepayment'
                then 'Principal repayments on'
            when {{ financial_flow_type }} = 'dcid:ServicingPayment'
                then 'Debt service on'
            when {{ financial_position_type }} = 'dcid:AmountOutstanding'
                then 'Outstanding'
            when {{ financial_position_type }} = 'dcid:AmountUndisbursed'
                then 'Undisbursed'
            when {{ financial_position_type }} = 'dcid:PrincipalArrears'
                then 'Principal arrears on'
            when {{ financial_position_type }} = 'dcid:InterestArrears'
                then 'Interest arrears on'
            -- Any other flow, e.g. a rescheduling, is named for its flow type.
            else
                upper(left({{ labels.financial_flow_type }}, 1))
                || substr({{ labels.financial_flow_type }}, 2)
                || ' of'
        end,
        -- Terms, then the instrument, e.g. `long-term concessional external debt`.
        case
            when {{ population_type }} <> 'dcid:EconomicUnit'
                then
                    concat_ws(
                        ' ',
                        regexp_replace({{ labels.maturity_band }}, ' maturity$', ''),
                        {{ labels.ids_concessionality }},
                        {{ labels.guarantee_status }},
                        case when {{ is_grant }} then 'grants' else 'external debt' end
                    )
                    || coalesce(' denominated in ' || {{ labels.currency_of_denomination }}, '')
                    || coalesce(' (' || nullif({{ qualifiers }}, '') || ')', '')
        end,
        -- Parties: who owes whom.
        'owed by the ' || {{ labels.ids_debtor_type }},
        'to ' || case
            when
                {{ labels.ids_creditor_type }}
                in ('bilateral', 'multilateral', 'other multilateral')
                then {{ labels.ids_creditor_type }} || ' creditors'
            else {{ labels.ids_creditor_type }}
        end
    )
    -- Creditor breakdown.
    || case when {{ creditor_breakdown }} is not null then ', by creditor' else '' end
    -- Unit, and the denominator of a ratio.
    || ' ('
    || case {{ unit }}
        when 'dcid:CurrencyUSD' then 'current US$'
        when 'dcid:Percent' then '%'
        when 'dcid:Year' then 'years'
    end
    || case
        when {{ denominator }} is null then ''
        when {{ denominator }} = 'dcid:Count_Person' then ' per person'
        else ' of ' || case {{ denominator }}
            when 'NY.GNP.MKTP.CD' then 'GNI'
            when 'DT.DOD.DECT.CD' then 'total external debt'
            when 'DT.DOD.DPPG.CD' then 'public or publicly guaranteed external debt'
        end
        -- A variant itemised by creditor divides by that creditor's total.
        || case
            when {{ creditor_breakdown }} is not null then ' owed to the same creditor'
            else ''
        end
    end
    || ')'
{%- endmacro %}
