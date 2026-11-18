{#-
  Use <prefix>_<layer> (e.g. statsbomb_staging, statsbomb_marts) instead of dbt's
  default <target_schema>_<custom>. The prefix matches the Glue databases that
  Terraform creates, so dbt never has to create databases in production.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ env_var('DBT_SCHEMA_PREFIX', 'statsbomb') }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
