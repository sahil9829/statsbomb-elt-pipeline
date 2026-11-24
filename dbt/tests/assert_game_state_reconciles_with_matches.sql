-- Splitting a match by game state must not lose or invent anything: per team, the
-- game-state rows have to add back up to the match fact's goals, shots and xG.
with by_state as (
    select match_id, team_key, sum(goals) as goals, sum(shots) as shots, sum(xg) as xg
    from {{ ref('fct_team_game_state') }}
    group by 1, 2
),

by_match as (
    select match_id, home_team_key as team_key, home_score as goals, home_shots as shots, home_xg as xg
    from {{ ref('fct_matches') }}
    union all
    select match_id, away_team_key, away_score, away_shots, away_xg
    from {{ ref('fct_matches') }}
)

select m.*, s.goals as state_goals, s.shots as state_shots, s.xg as state_xg
from by_match as m
left join by_state as s
    on m.match_id = s.match_id
    and m.team_key = s.team_key
where coalesce(s.goals, -1) <> m.goals
   or coalesce(s.shots, -1) <> m.shots
   or abs(coalesce(s.xg, -1) - m.xg) > 0.01
