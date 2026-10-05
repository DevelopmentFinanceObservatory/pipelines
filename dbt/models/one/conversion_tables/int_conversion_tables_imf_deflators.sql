-- Each entity's IMF-table deflator rebased to equal 100 in the deflator base
-- year. The rule is in the conversion_tables_rebased_deflators macro.
{{
    conversion_tables_rebased_deflators(
        deflators_unrebased=ref("int_conversion_tables_imf_deflators_unrebased"),
    )
}}
