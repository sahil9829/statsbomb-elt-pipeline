-- Grain: one row per on-pitch event (shoot-out excluded), tagged with the
-- scoreline *from the acting team's point of view* at the moment it happened.
--
-- "Game state" is the score difference before the event: a shot that scores is
-- taken while e.g. Drawing, and everything after it happens while Winning by 1.
-- Own goals count for the benefiting team (StatsBomb 'Own Goal For'), the same
-- rule the score-reconciliation test uses.
--
-- Events are ordered by clock time, not event_index: a few StatsBomb events carry
-- an index far from their timestamp (e.g. a 45:00 ball receipt indexed late in the
-- half), which would otherwise create phantom 50-minute gaps.
with events as (
    select * from {{ ref('stg_statsbomb__events') }}
    where period < 5
),

match_teams as (
    select * from {{ ref('int_match_teams') }}
),

goal_deltas as (
    select
        e.*,
        mt.is_home,
        mt.opponent_team_id,
        -- +1 when the home side scores, -1 when the away side scores
        case
            when e.is_goal or e.is_own_goal_for
                then case when mt.is_home then 1 else -1 end
            else 0
        end as home_goal_delta
    from events as e
    left join match_teams as mt
        on e.match_id = mt.match_id
        and e.team_id = mt.team_id
),

running as (
    select
        *,
        coalesce(sum(home_goal_delta) over (
            partition by match_id order by period, match_clock_minutes, event_index
            rows between unbounded preceding and 1 preceding
        ), 0)                                                       as home_goal_diff_before,
        sum(home_goal_delta) over (
            partition by match_id order by period, match_clock_minutes, event_index
            rows between unbounded preceding and current row
        )                                                           as home_goal_diff_after,
        -- Time until the next event in the same period. StatsBomb's clock restarts at
        -- 45:00 / 90:00 / 105:00, so gaps are never measured across a period boundary.
        greatest(
            coalesce(lead(match_clock_minutes) over (
                partition by match_id, period order by match_clock_minutes, event_index
            ) - match_clock_minutes, 0),
            0
        )                                                           as minutes_to_next_event
    from goal_deltas
)

select
    *,
    case when is_home then home_goal_diff_before else -home_goal_diff_before end as goal_diff_before
from running
