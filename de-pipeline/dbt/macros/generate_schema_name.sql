{#
  Use the model's configured +schema (silver_crypto / gold_crypto) verbatim,
  instead of dbt's default of prefixing it with the target schema
  (e.g. public_silver_crypto). Environment separation comes from the database
  (DEV_DB / QA_DB / PROD_DB), not from a schema-name prefix.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
