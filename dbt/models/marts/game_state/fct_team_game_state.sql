-- Grain: one row per match x team x game state (5 levels, see dim_game_state).
--
-- Every measure is additive (a count or a sum), so BI tools can roll it up to any
-- level and then divide: e.g. shots per 90 = sum(shots) / sum(minutes_in_state) * 90,
-- average pressure height = sum(pressure_x_sum) / sum(pressures). Never store an
-- average here: averaging averages across rows gives the wrong answer.
with events as (
    select * from {{ ref('int_event_game_state') }}
),

match_teams as (
    select * from {{ ref('int_match_teams') }}
),

-- Clock time is shared by both sides: attribute each gap to the state *after* the event
-- (from the home side's view), then mirror it onto each team.
match_time as (
    select match_id, home_goal_diff_after, sum(minutes_to_next_event) as minutes
    from events
    group by match_id, home_goal_diff_after
),

team_time as (
    select
        mt.match_id,
        mt.team_id,
        mt.opponent_team_id,
        mt.is_home,
        {{ game_state_key('case when mt.is_home then t.home_goal_diff_after else -t.home_goal_diff_after end') }} as game_state_key,
        sum(t.minutes) as minutes_in_state
    from match_time as t
    inner join match_teams as mt
        on t.match_id = mt.match_id
    group by 1, 2, 3, 4, 5
),

team_events as (
    select
        match_id,
        team_id,
        {{ game_state_key('goal_diff_before') }}                                        as game_state_key,
        sum({{ flag('is_shot_in_play') }})                                              as shots,
        sum({{ flag('is_shot_in_play and is_shot_on_target') }})                        as shots_on_target,
        sum({{ flag('is_goal or is_own_goal_for') }})                                   as goals,
        coalesce(sum(case when is_shot_in_play then shot_xg end), 0)                    as xg,
        sum({{ flag('is_pass') }})                                                      as passes,
        sum({{ flag('is_pass_completed') }})                                            as passes_completed,
        sum({{ flag('is_pass and pass_length >= 35') }})                                as long_passes,
        -- PPDA inputs (StatsBomb pitch is 120 long; each team attacks towards x = 120)
        sum({{ flag('is_pass and location_x < 72') }})                                  as passes_own_60pct,
        sum({{ flag("location_x >= 48 and (event_type_name in ('Interception', 'Foul Committed') or (event_type_name = 'Duel' and duel_type_name = 'Tackle'))") }}) as def_actions_opp_60pct,
        sum({{ flag("event_type_name = 'Pressure'") }})                                 as pressures,
        coalesce(sum(case when event_type_name = 'Pressure' then location_x end), 0)   as pressure_x_sum,
        sum({{ flag("event_type_name = 'Pressure' and location_x >= 80") }})            as pressures_final_third
    from events
    where team_id is not null
    group by 1, 2, 3
),

combined as (
    select
        coalesce(tt.match_id, te.match_id)                    as match_id,
        coalesce(tt.team_id, te.team_id)                      as team_id,
        coalesce(tt.game_state_key, te.game_state_key)        as game_state_key,
        coalesce(tt.minutes_in_state, 0)                      as minutes_in_state,
        coalesce(te.shots, 0)                                 as shots,
        coalesce(te.shots_on_target, 0)                       as shots_on_target,
        coalesce(te.goals, 0)                                 as goals,
        coalesce(te.xg, 0)                                    as xg,
        coalesce(te.passes, 0)                                as passes,
        coalesce(te.passes_completed, 0)                      as passes_completed,
        coalesce(te.long_passes, 0)                           as long_passes,
        coalesce(te.passes_own_60pct, 0)                      as passes_own_60pct,
        coalesce(te.def_actions_opp_60pct, 0)                 as def_actions_opp_60pct,
        coalesce(te.pressures, 0)                             as pressures,
        coalesce(te.pressure_x_sum, 0)                        as pressure_x_sum,
        coalesce(te.pressures_final_third, 0)                 as pressures_final_third
    from team_time as tt
    full outer join team_events as te
        on tt.match_id = te.match_id
        and tt.team_id = te.team_id
        and tt.game_state_key = te.game_state_key
),

with_opponent as (
    select c.*, mt.opponent_team_id, mt.is_home
    from combined as c
    inner join match_teams as mt
        on c.match_id = mt.match_id
        and c.team_id = mt.team_id
),

matches as (
    select match_id, match_date, competition_id, season_id
    from {{ ref('stg_statsbomb__matches') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['t.match_id', 't.team_id', 't.game_state_key']) }} as team_game_state_key,
    t.match_id,
    {{ date_key('m.match_date') }}                                      as match_date_key,
    {{ dbt_utils.generate_surrogate_key(['m.competition_id', 'm.season_id']) }} as competition_key,
    {{ dim_key(['t.team_id']) }}                                        as team_key,
    {{ dim_key(['t.opponent_team_id']) }}                               as opponent_team_key,
    t.game_state_key,
    t.is_home,
    round(t.minutes_in_state, 3)                                        as minutes_in_state,
    t.shots,
    t.shots_on_target,
    t.goals,
    round(t.xg, 4)                                                      as xg,
    t.passes,
    t.passes_completed,
    t.long_passes,
    t.passes_own_60pct,
    t.def_actions_opp_60pct,
    t.pressures,
    round(t.pressure_x_sum, 2)                                          as pressure_x_sum,
    t.pressures_final_third,
    -- the opponent's numbers in the mirrored state (Winning by 1 <-> Losing by 1)
    coalesce(o.shots, 0)                                                as shots_conceded,
    coalesce(o.goals, 0)                                                as goals_conceded,
    round(coalesce(o.xg, 0), 4)                                         as xg_conceded,
    coalesce(o.passes, 0)                                               as opponent_passes,
    coalesce(o.passes_own_60pct, 0)                                     as opponent_passes_own_60pct
from with_opponent as t
inner join matches as m
    on t.match_id = m.match_id
left join with_opponent as o
    on t.match_id = o.match_id
    and t.opponent_team_id = o.team_id
    and t.game_state_key = -o.game_state_key
