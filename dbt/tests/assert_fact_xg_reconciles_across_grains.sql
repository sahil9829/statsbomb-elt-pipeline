-- Total xG must agree between the event, player-match and match facts
-- (tolerance covers per-row rounding in the aggregate facts).
with by_event as (
    select match_id, sum(shot_xg) as xg
    from {{ ref('fct_events') }}
    where is_shot = 1 and period < 5
    group by match_id
),

by_player as (
    select match_id, sum(xg) as xg
    from {{ ref('fct_player_match') }}
    group by match_id
),

by_match as (
    select match_id, home_xg + away_xg as xg
    from {{ ref('fct_matches') }}
)

select m.match_id, e.xg as event_xg, p.xg as player_xg, m.xg as match_xg
from by_match as m
left join by_event as e on m.match_id = e.match_id
left join by_player as p on m.match_id = p.match_id
where abs(coalesce(e.xg, 0) - m.xg) > 0.01
   or abs(coalesce(p.xg, 0) - m.xg) > 0.05
