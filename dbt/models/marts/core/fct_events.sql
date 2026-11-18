-- Grain: one row per on-pitch event.
with events as (
    select * from {{ ref('stg_statsbomb__events') }}
),

matches as (
    select match_id, match_date, competition_id, season_id
    from {{ ref('stg_statsbomb__matches') }}
),

match_teams as (
    select * from {{ ref('int_match_teams') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['e.event_id']) }}              as event_key,
    e.event_id,
    e.match_id,

    -- dimension foreign keys
    {{ date_key('m.match_date') }}                                      as match_date_key,
    {{ dbt_utils.generate_surrogate_key(['m.competition_id', 'm.season_id']) }} as competition_key,
    {{ dim_key(['e.team_id']) }}                                        as team_key,
    {{ dim_key(['mt.opponent_team_id']) }}                              as opponent_team_key,
    {{ dim_key(['e.player_id']) }}                                      as player_key,
    {{ dbt_utils.generate_surrogate_key(['e.event_type_id']) }}         as event_type_key,
    {{ dbt_utils.generate_surrogate_key(['e.play_pattern_id']) }}       as play_pattern_key,

    -- degenerate dimensions / event context
    e.event_index,
    e.period,
    e.minute,
    e.second,
    e.match_clock_minutes,
    e.possession,
    e.possession_team_id = e.team_id                                    as is_in_possession,
    mt.is_home,
    e.position_name,
    e.pass_height_name,
    e.pass_type_name,
    e.shot_outcome_name,
    e.shot_body_part_name,
    e.shot_type_name,
    e.card_name,

    -- spatial measures (StatsBomb 120 x 80 pitch, attacking left to right)
    e.location_x,
    e.location_y,
    e.end_location_x,
    e.end_location_y,
    case
        when e.is_shot then round(sqrt(power(120 - e.location_x, 2) + power(40 - e.location_y, 2)), 2)
    end                                                                 as shot_distance_to_goal,

    -- additive measures
    e.duration,
    e.pass_length,
    e.shot_xg,
    {{ flag('e.is_pass') }}                                             as is_pass,
    {{ flag('e.is_pass_completed') }}                                   as is_pass_completed,
    {{ flag('e.pass_is_shot_assist') }}                                 as is_key_pass,
    {{ flag('e.pass_is_goal_assist') }}                                 as is_assist,
    {{ flag('e.is_shot') }}                                             as is_shot,
    {{ flag('e.is_shot_on_target') }}                                   as is_shot_on_target,
    {{ flag('e.is_goal') }}                                             as is_goal,
    {{ flag('e.is_under_pressure') }}                                   as is_under_pressure
from events as e
inner join matches as m
    on e.match_id = m.match_id
left join match_teams as mt
    on e.match_id = mt.match_id
    and e.team_id = mt.team_id
