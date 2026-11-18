-- Grain: one row per competition-season (e.g. "FIFA World Cup 2018").
with competitions as (
    select * from {{ ref('stg_statsbomb__competitions') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['competition_id', 'season_id']) }} as competition_key,
    competition_id,
    season_id,
    competition_name,
    season_name,
    competition_name || ' ' || season_name      as competition_season_name,
    season_start_year,
    competition_country_name,
    competition_gender,
    is_youth,
    is_international,
    match_updated_at
from competitions
