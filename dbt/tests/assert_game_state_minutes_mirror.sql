-- Game state is symmetric: whenever one team is Winning by 1, the other is Losing by 1
-- for exactly the same minutes. Also checks that time in state roughly covers a match.
with t as (
    select match_id, team_key, opponent_team_key, game_state_key, minutes_in_state
    from {{ ref('fct_team_game_state') }}
),

mirror_breaks as (
    select a.match_id, a.team_key, a.game_state_key, a.minutes_in_state, b.minutes_in_state as mirror_minutes
    from t as a
    left join t as b
        on a.match_id = b.match_id
        and a.opponent_team_key = b.team_key
        and a.game_state_key = -b.game_state_key
    where abs(a.minutes_in_state - coalesce(b.minutes_in_state, 0)) > 0.001
),

too_short as (
    select match_id, team_key, null as game_state_key, sum(minutes_in_state) as minutes_in_state, null as mirror_minutes
    from t
    group by 1, 2
    having sum(minutes_in_state) < 85
)

select * from mirror_breaks
union all
select * from too_short
