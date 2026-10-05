{% macro is_dac1_donor(donor_code) -%}
    {#-
        Whether a DAC1 donor code names a donor the conversion tables keep. DAC1
        numbers every country and organisation below 20000 and every group total
        from 20000 up (DAC Countries, Total; Multilaterals, Total; G7 Countries,
        Total; and others), so a group total is left out by its code range, with
        no list to keep current. DAC Countries, Total (20001) is kept, as the
        deflator entity the DAC recipe falls back to.
    -#}
    (cast({{ donor_code }} as integer) < 20000 or {{ donor_code }} = '20001')
{%- endmacro %}
