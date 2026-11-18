with source as (
    select * from {{ source('statsbomb', 'lineups') }}
)

select
    match_id,
    team_id,
    team_name,
    player_id,
    player_name,
    player_nickname,
    coalesce(player_nickname, player_name)  as player_display_name,
    jersey_number,
    country_id                              as player_country_id,
    country_name                            as player_country_name,
    appeared,
    is_starter,
    first_position_id,
    first_position_name,
    on_minute,
    on_period,
    off_minute,
    off_period,
    off_reason,
    yellow_cards,
    red_cards,
    cast(_ingested_at as timestamp)         as _ingested_at
from source
