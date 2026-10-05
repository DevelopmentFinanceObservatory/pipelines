-- Each DAC1 donor sharing a currency, compared with the currency's reference
-- donor in the member's last actual year. A member's DAC1 rows are all reported,
-- so its last actual year is its latest one.
{{
    conversion_tables_rate_agreement(
        recipe="dcid:ConversionRecipeDAC",
        rate_members=ref("int_conversion_tables_dac_rate_members"),
        rate_references=ref("int_conversion_tables_dac_rate_references"),
        rates=ref("int_conversion_tables_dac_rates"),
    )
}}
