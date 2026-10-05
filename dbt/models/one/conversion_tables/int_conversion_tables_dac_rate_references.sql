-- The area whose rate is each currency's rate in the DAC table, one row per
-- currency. The rule is in the conversion_tables_rate_references macro.
{{
    conversion_tables_rate_references(
        recipe="dcid:ConversionRecipeDAC",
        rate_members=ref("int_conversion_tables_dac_rate_members"),
    )
}}
