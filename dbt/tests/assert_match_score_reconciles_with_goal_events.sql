-- The published score must equal goals scored from shots (excluding the shoot-out)
-- plus own goals credited to each side. A mismatch means events are missing or
-- the period-5 / own-goal handling in staging is wrong.
with goals as (
    select match_id, team_id, goals
    from {{ ref('int_match_team_stats') }}
)

select
    m.match_id,
    m.home_score,
    coalesce(h.goals, 0) as home_goals_from_events,
    m.away_score,
    coalesce(a.goals, 0) as away_goals_from_events
from {{ ref('stg_statsbomb__matches') }} as m
left join goals as h on m.match_id = h.match_id and m.home_team_id = h.team_id
left join goals as a on m.match_id = a.match_id and m.away_team_id = a.team_id
where m.home_score <> coalesce(h.goals, 0)
   or m.away_score <> coalesce(a.goals, 0)
