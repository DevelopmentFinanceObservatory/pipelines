-- Each entity's DAC-table deflator rebased to equal 100 in the deflator base
-- year. The rule is in the conversion_tables_rebased_deflators macro. A donor
-- with no base-year deflator is left out, because the DAC never rebases a donor
-- that has stopped reporting and such a series has no known base. The DAC
-- recipe deflates those donors' rows by DAC Countries, Total.
{{
    conversion_tables_rebased_deflators(
        deflators_unrebased=ref("int_conversion_tables_dac_deflators_unrebased"),
    )
}}
