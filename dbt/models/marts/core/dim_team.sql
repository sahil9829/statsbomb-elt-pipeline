-- Grain: one row per team. Attributes come from the most recent match the team played.
with sides as (
    select match_date, home_team_id as team_id, home_team_name as team_name,
           home_team_gender as team_gender, home_team_country_name as team_country_name
    from {{ ref('stg_statsbomb__matches') }}
    union all
    select match_date, away_team_id, away_team_name, away_team_gender, away_team_country_name
    from {{ ref('stg_statsbomb__matches') }}
),

latest as (
    select
        *,
        row_number() over (partition by team_id order by match_date desc) as rn
    from sides
),

teams as (
    select team_id, team_name, team_gender, team_country_name
    from latest
    where rn = 1

    union all

    select
        cast({{ var('unknown_id') }} as bigint),
        'Unknown',
        cast(null as {{ dbt.type_string() }}),
        cast(null as {{ dbt.type_string() }})
)

select
    {{ dim_key(['team_id']) }} as team_key,
    team_id,
    team_name,
    team_gender,
    team_country_name,
    team_id = {{ var('unknown_id') }} as is_unknown_member
from teams
