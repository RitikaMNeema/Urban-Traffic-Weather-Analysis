-- Custom schema macro to prevent concatenation with target schema
-- Path: /opt/airflow/dbt/macros/generate_schema_name.sql
-- 
-- By default, dbt concatenates: target_schema + "_" + custom_schema
-- This macro overrides that behavior to use ONLY the custom schema name
-- 
-- Example without this macro:
--   target.schema = "RAW_ARCHIVE"
--   config(schema="MARTS") 
--   Result: RAW_ARCHIVE_MARTS ❌
--
-- Example WITH this macro:
--   target.schema = "RAW_ARCHIVE"  
--   config(schema="MARTS")
--   Result: MARTS ✅

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {# If no custom schema specified, use target schema #}
        {{ default_schema }}
    {%- else -%}
        {# Use ONLY the custom schema name without concatenation #}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}