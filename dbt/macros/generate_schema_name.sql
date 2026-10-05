{#
    dbt's default prefixes a configured +schema with target.schema, so oecd_dac_crs
    would build as main_oecd_dac_crs locally and <dataset>_oecd_dac_crs on BigQuery.
    Rendering the configured schema verbatim keeps relation names identical across
    targets: it is the DuckDB schema locally and the BigQuery dataset on GCP, so
    environments are separated by connection rather than by name.

    Erroring on a missing +schema stops a resource silently landing in the target's
    default schema, away from the raw tables Dagster loads.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ exceptions.raise_compiler_error(
            "No +schema set for " ~ node.unique_id ~ ". Every resource must declare its <source>_<dataset> schema."
        ) }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
