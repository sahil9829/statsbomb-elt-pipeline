with source as (
    select * from {{ source('statsbomb', 'matches') }}
)

select
    match_id,
    match_date,
    kick_off,
    competition_id,
    season_id,
    home_team_id,
    home_team_name,
    home_team_gender,
    home_team_country_name,
    home_manager_name,
    away_team_id,
    away_team_name,
    away_team_gender,
    away_team_country_name,
    away_manager_name,
    home_score,
    away_score,
    match_status,
    match_week,
    competition_stage_id,
    competition_stage_name,
    stadium_id,
    stadium_name,
    stadium_country_name,
    referee_id,
    referee_name,
    referee_country_name,
    data_version,
    cast(last_updated_at as timestamp)  as last_updated_at,
    cast(_ingested_at as timestamp)     as _ingested_at
from source
