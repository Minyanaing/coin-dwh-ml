{% macro snowflake__create_schema(relation) %}
  {#-
    Schemas (SILVER_CRYPTO / GOLD_CRYPTO in DEV_DB / QA_DB / PROD_DB) are
    pre-created and owned by snowflake/setup.sql. dbt must NOT create schemas —
    it only builds tables/views into the existing ones. This override replaces
    dbt's default `create schema if not exists` with a no-op.

    generate_schema_name() already points dbt at those exact schema names; this
    macro guarantees dbt never issues a CREATE SCHEMA. If a target schema is
    genuinely missing, model creation fails loudly — the signal to run setup.sql.
  -#}
  {%- if execute -%}
    {{ log("create_schema skipped for " ~ relation ~ " — schemas are managed by snowflake/setup.sql", info=false) }}
  {%- endif -%}
{% endmacro %}
