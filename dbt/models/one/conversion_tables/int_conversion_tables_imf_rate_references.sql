-- The area whose rate is each currency's rate in the IMF table, one row per
-- currency. The rule is in the conversion_tables_rate_references macro.
{{
    conversion_tables_rate_references(
        recipe="dcid:ConversionRecipeIMF",
        rate_members=ref("int_conversion_tables_imf_rate_members"),
    )
}}
