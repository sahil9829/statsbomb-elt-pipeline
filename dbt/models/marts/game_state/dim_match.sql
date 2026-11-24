-- Match dimension for BI: one row per match with a readable label, e.g.
-- "Argentina 3-3 France (Final)", so slicers and tooltips don't show raw ids.
with matches as (
    select * from {{ ref('stg_statsbomb__matches') }}
),

teams as (
    select team_id, team_name from {{ ref('dim_team') }}
),

lengths as (
    select * from {{ ref('int_match_lengths') }}
)

select
    m.match_id,
    m.match_date,
    m.competition_stage_name,
    case
        when m.competition_stage_name = 'Group Stage' then 1
        when m.competition_stage_name = 'Round of 16' then 2
        when m.competition_stage_name = 'Quarter-finals' then 3
        when m.competition_stage_name = 'Semi-finals' then 4
        when m.competition_stage_name = '3rd Place Final' then 5
        when m.competition_stage_name = 'Final' then 6
        else 9
    end                                                         as stage_sort,
    home_t.team_name                                                as home_team_name,
    away_t.team_name                                                as away_team_name,
    m.home_score,
    m.away_score,
    home_t.team_name || ' ' || cast(m.home_score as varchar) || '-'
        || cast(m.away_score as varchar) || ' ' || away_t.team_name as match_label,
    coalesce(l.went_to_extra_time, false)                       as went_to_extra_time,
    coalesce(l.went_to_penalties, false)                        as went_to_penalties
from matches as m
left join teams as home_t on m.home_team_id = home_t.team_id
left join teams as away_t on m.away_team_id = away_t.team_id
left join lengths as l on m.match_id = l.match_id
