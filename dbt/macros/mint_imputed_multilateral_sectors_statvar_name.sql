{% macro mint_imputed_multilateral_sectors_statvar_name(purpose_label) %}
    {#-
        Human-readable name for an imputed multilateral sectors StatVar:
        `Imputed multilateral ODA gross disbursements for <purpose>`

        Every row shares the same population type, netting basis, flow type and
        flow category, so the name has only the purpose to vary on. This macro
        is easy to confuse with mint_crs_statvar_name, which mints names from
        finance and modality labels that this dataset does not carry.
    -#}
    'Imputed multilateral ODA gross disbursements for '
    || {{ strip_label_parenthetical(purpose_label) }}
{%- endmacro %}
