-- Team-level attacking totals per match, pivoted onto the match fact.
with events as (
    select * from {{ ref('stg_statsbomb__events') }}
)

select
    match_id,
    team_id,
    sum({{ flag('is_shot_in_play') }})                                  as shots,
    sum({{ flag('is_shot_in_play and is_shot_on_target') }})            as shots_on_target,
    sum({{ flag('is_goal') }}) + sum({{ flag('is_own_goal_for') }})     as goals,
    coalesce(sum(case when is_shot_in_play then shot_xg end), 0)        as xg,
    sum({{ flag('is_pass') }})                                          as passes,
    sum({{ flag('is_pass_completed') }})                                as passes_completed
from events
group by match_id, team_id
