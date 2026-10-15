{% macro generate_schema_name(custom_schema_name, node) -%}
    {#- Use the custom schema as-is (e.g. STAGING, MART) instead of dbt's default <target>_<custom> -#}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
