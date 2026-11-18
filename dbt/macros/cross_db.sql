{#- Small cross-database helpers so the same SQL runs on Athena (Trino) and DuckDB. -#}

{# ISO day of week: Monday = 1 ... Sunday = 7 #}
{% macro iso_day_of_week(date_expr) -%}
    {{ return(adapter.dispatch('iso_day_of_week', 'statsbomb')(date_expr)) }}
{%- endmacro %}

{% macro default__iso_day_of_week(date_expr) -%}
    extract(isodow from {{ date_expr }})
{%- endmacro %}

{% macro athena__iso_day_of_week(date_expr) -%}
    day_of_week({{ date_expr }})
{%- endmacro %}

{# yyyymmdd integer key for joining facts to dim_date #}
{% macro date_key(date_expr) -%}
    cast(
        extract(year from {{ date_expr }}) * 10000
        + extract(month from {{ date_expr }}) * 100
        + extract(day from {{ date_expr }})
    as integer)
{%- endmacro %}

{# 1/0 flag from a boolean expression, for additive fact measures #}
{% macro flag(condition) -%}
    case when {{ condition }} then 1 else 0 end
{%- endmacro %}

{#
  Surrogate key that maps a missing natural key to the dimension's unknown member,
  so every fact foreign key is NOT NULL and joins cleanly.
#}
{% macro dim_key(columns) -%}
    {%- set coalesced = [] -%}
    {%- for c in columns -%}
        {%- do coalesced.append("coalesce(" ~ c ~ ", " ~ var('unknown_id') ~ ")") -%}
    {%- endfor -%}
    {{ dbt_utils.generate_surrogate_key(coalesced) }}
{%- endmacro %}
