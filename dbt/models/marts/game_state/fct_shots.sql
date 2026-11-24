-- Grain: one row per shot in play (shoot-out excluded), with the game state it was taken in.
with events as (
    select * from {{ ref('int_event_game_state') }}
    where is_shot_in_play
),

matches as (
    select match_id, match_date, competition_id, season_id
    from {{ ref('stg_statsbomb__matches') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['e.event_id']) }}              as shot_key,
    e.event_id,
    e.match_id,
    {{ date_key('m.match_date') }}                                      as match_date_key,
    {{ dbt_utils.generate_surrogate_key(['m.competition_id', 'm.season_id']) }} as competition_key,
    {{ dim_key(['e.team_id']) }}                                        as team_key,
    {{ dim_key(['e.opponent_team_id']) }}                               as opponent_team_key,
    {{ dim_key(['e.player_id']) }}                                      as player_key,
    {{ game_state_key('e.goal_diff_before') }}                          as game_state_key,
    e.goal_diff_before,
    e.is_home,
    e.period,
    e.minute,
    e.second,
    e.play_pattern_name,
    e.shot_type_name,
    e.shot_body_part_name,
    e.shot_technique_name,
    e.shot_outcome_name,
    e.shot_first_time,
    e.is_under_pressure,
    e.location_x,
    e.location_y,
    round(sqrt(power(120 - e.location_x, 2) + power(40 - e.location_y, 2)), 2) as shot_distance_to_goal,
    e.shot_xg                                                           as xg,
    {{ flag('e.is_goal') }}                                             as is_goal,
    {{ flag('e.is_shot_on_target') }}                                   as is_on_target
from events as e
inner join matches as m
    on e.match_id = m.match_id
