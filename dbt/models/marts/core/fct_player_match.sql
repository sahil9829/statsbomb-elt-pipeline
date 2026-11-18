-- Grain: one row per player per match (full squad, including unused substitutes).
with stats as (
    select * from {{ ref('int_player_match_stats') }}
),

matches as (
    select match_id, match_date, competition_id, season_id
    from {{ ref('stg_statsbomb__matches') }}
),

match_teams as (
    select * from {{ ref('int_match_teams') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['s.match_id', 's.player_id']) }} as player_match_key,
    s.match_id,

    -- dimension foreign keys
    {{ date_key('m.match_date') }}                                      as match_date_key,
    {{ dbt_utils.generate_surrogate_key(['m.competition_id', 'm.season_id']) }} as competition_key,
    {{ dim_key(['s.player_id']) }}                                      as player_key,
    {{ dim_key(['s.team_id']) }}                                        as team_key,
    {{ dim_key(['mt.opponent_team_id']) }}                              as opponent_team_key,

    -- context
    mt.is_home,
    s.is_starter,
    s.appeared,
    s.first_position_name                                               as starting_position,

    -- measures
    s.minutes_played,
    s.events,
    s.passes,
    s.passes_completed,
    case when s.passes > 0 then round(1.0 * s.passes_completed / s.passes, 3) end as pass_completion_rate,
    s.key_passes,
    s.assists,
    s.shots,
    s.shots_on_target,
    s.goals,
    round(s.xg, 3)                                                      as xg,
    s.dribbles,
    s.dribbles_completed,
    s.tackles,
    s.interceptions,
    s.pressures,
    s.ball_recoveries,
    s.yellow_cards,
    s.red_cards
from stats as s
inner join matches as m
    on s.match_id = m.match_id
left join match_teams as mt
    on s.match_id = mt.match_id
    and s.team_id = mt.team_id
