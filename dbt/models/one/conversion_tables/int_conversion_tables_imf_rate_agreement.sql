-- Each WEO country sharing a currency, compared with the currency's reference
-- country in the member's last actual year.
{{
    conversion_tables_rate_agreement(
        recipe="dcid:ConversionRecipeIMF",
        rate_members=ref("int_conversion_tables_imf_rate_members"),
        rate_references=ref("int_conversion_tables_imf_rate_references"),
        rates=ref("int_conversion_tables_imf_rates"),
    )
}}
