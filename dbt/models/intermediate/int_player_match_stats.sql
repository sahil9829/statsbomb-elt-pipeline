-- Player x match: lineup facts (starter, minutes, cards) joined to on-ball event totals.
with lineups as (
    select * from {{ ref('stg_statsbomb__lineups') }}
),

match_lengths as (
    select * from {{ ref('int_match_lengths') }}
),

player_events as (
    select
        match_id,
        player_id,
        count(*)                                                      as events,
        sum({{ flag('is_pass') }})                                    as passes,
        sum({{ flag('is_pass_completed') }})                          as passes_completed,
        sum({{ flag('pass_is_shot_assist') }})                        as key_passes,
        sum({{ flag('pass_is_goal_assist') }})                        as assists,
        sum({{ flag('is_shot_in_play') }})                            as shots,
        sum({{ flag('is_shot_in_play and is_shot_on_target') }})      as shots_on_target,
        sum({{ flag('is_goal') }})                                    as goals,
        coalesce(sum(case when is_shot_in_play then shot_xg end), 0)  as xg,
        sum({{ flag("event_type_name = 'Dribble'") }})                as dribbles,
        sum({{ flag("event_type_name = 'Dribble' and dribble_outcome_name = 'Complete'") }}) as dribbles_completed,
        sum({{ flag("event_type_name = 'Duel' and duel_type_name = 'Tackle'") }})          as tackles,
        sum({{ flag("event_type_name = 'Interception'") }})          as interceptions,
        sum({{ flag("event_type_name = 'Pressure'") }})              as pressures,
        sum({{ flag("event_type_name = 'Ball Recovery'") }})         as ball_recoveries
    from {{ ref('stg_statsbomb__events') }}
    where player_id is not null
    group by match_id, player_id
)

select
    l.match_id,
    l.team_id,
    l.player_id,
    l.is_starter,
    l.appeared,
    l.first_position_name,
    case
        when not l.appeared then 0
        else greatest(
            0,
            round(coalesce(l.off_minute, ml.match_length_minutes) - l.on_minute, 1)
        )
    end                                         as minutes_played,
    l.yellow_cards,
    l.red_cards,
    coalesce(pe.events, 0)                      as events,
    coalesce(pe.passes, 0)                      as passes,
    coalesce(pe.passes_completed, 0)            as passes_completed,
    coalesce(pe.key_passes, 0)                  as key_passes,
    coalesce(pe.assists, 0)                     as assists,
    coalesce(pe.shots, 0)                       as shots,
    coalesce(pe.shots_on_target, 0)             as shots_on_target,
    coalesce(pe.goals, 0)                       as goals,
    coalesce(pe.xg, 0)                          as xg,
    coalesce(pe.dribbles, 0)                    as dribbles,
    coalesce(pe.dribbles_completed, 0)          as dribbles_completed,
    coalesce(pe.tackles, 0)                     as tackles,
    coalesce(pe.interceptions, 0)               as interceptions,
    coalesce(pe.pressures, 0)                   as pressures,
    coalesce(pe.ball_recoveries, 0)             as ball_recoveries
from lineups as l
left join match_lengths as ml
    on l.match_id = ml.match_id
left join player_events as pe
    on l.match_id = pe.match_id
    and l.player_id = pe.player_id
