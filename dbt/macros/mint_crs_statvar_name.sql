{% macro mint_crs_statvar_name(
    type_of_flow_label, type_of_finance_label, cooperation_modality_label,
    purpose_label, measured_property, financial_obligation_type,
    financial_flow_type, netting_basis
) %}
    {#-
        Human-readable name for a CRS StatVar:
        `<flow> <measure> for <purpose> (<finance>[, <modality>])`

        The measure collapses four columns into one phrase. The data holds four
        combinations - commitments, gross and net disbursements, and grant
        equivalents.

        concat_ws omits a null argument along with its separator, so a StatVar with
        no cooperation modality leaves no gap behind.
    -#}
    {%- set flow = strip_label_parenthetical(type_of_flow_label) -%}
    concat_ws(
        ' ',
        upper(left({{ flow }}, 1)) || substr({{ flow }}, 2),
        case
            when {{ financial_obligation_type }} = 'dcid:Commitment'
                then 'commitments'
            when
                {{ financial_flow_type }} = 'dcid:Disbursement'
                and {{ netting_basis }} = 'dcid:Gross'
                then 'gross disbursements'
            when
                {{ financial_flow_type }} = 'dcid:Disbursement'
                and {{ netting_basis }} = 'dcid:Net'
                then 'net disbursements'
            when {{ measured_property }} = 'dcid:grantEquivalent'
                then 'grant equivalents'
        end,
        'for',
        {{ strip_label_parenthetical(purpose_label) }}
    )
    || ' ('
    || concat_ws(
        ', ',
        {{ strip_label_parenthetical(type_of_finance_label) }},
        {{ strip_label_parenthetical(cooperation_modality_label) }}
    )
    || ')'
{%- endmacro %}
