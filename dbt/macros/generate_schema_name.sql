-- Override dbt's default schema naming behavior
-- Default: {target_schema}_{custom_schema} = bronze_bronze ❌
-- This macro: use custom_schema directly = bronze ✅

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}