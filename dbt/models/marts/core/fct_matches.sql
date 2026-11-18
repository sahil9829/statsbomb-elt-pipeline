-- Grain: one row per match.
with matches as (
    select * from {{ ref('stg_statsbomb__matches') }}
),

lengths as (
    select * from {{ ref('int_match_lengths') }}
),

team_stats as (
    select * from {{ ref('int_match_team_stats') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['m.match_id']) }}               as match_key,
    m.match_id,

    -- dimension foreign keys
    {{ date_key('m.match_date') }}                                      as match_date_key,
    {{ dbt_utils.generate_surrogate_key(['m.competition_id', 'm.season_id']) }} as competition_key,
    {{ dim_key(['m.home_team_id']) }}                                   as home_team_key,
    {{ dim_key(['m.away_team_id']) }}                                   as away_team_key,
    {{ dim_key(['m.stadium_id']) }}                                     as venue_key,
    {{ dim_key(['m.referee_id']) }}                                     as referee_key,

    -- degenerate dimensions
    m.kick_off,
    m.match_week,
    m.competition_stage_name,
    m.home_manager_name,
    m.away_manager_name,

    -- measures
    m.home_score,
    m.away_score,
    m.home_score + m.away_score                                         as total_goals,
    m.home_score - m.away_score                                         as goal_difference,
    case
        when m.home_score > m.away_score then 'home_win'
        when m.home_score < m.away_score then 'away_win'
        else 'draw'
    end                                                                 as match_result,
    coalesce(hs.shots, 0)                                               as home_shots,
    coalesce(aws.shots, 0)                                              as away_shots,
    coalesce(hs.shots_on_target, 0)                                     as home_shots_on_target,
    coalesce(aws.shots_on_target, 0)                                    as away_shots_on_target,
    round(coalesce(hs.xg, 0), 3)                                        as home_xg,
    round(coalesce(aws.xg, 0), 3)                                       as away_xg,
    coalesce(hs.passes, 0)                                              as home_passes,
    coalesce(aws.passes, 0)                                             as away_passes,
    l.match_length_minutes,
    coalesce(l.went_to_extra_time, false)                               as went_to_extra_time,
    coalesce(l.went_to_penalties, false)                                as went_to_penalties,
    m.last_updated_at
from matches as m
left join lengths as l
    on m.match_id = l.match_id
left join team_stats as hs
    on m.match_id = hs.match_id
    and m.home_team_id = hs.team_id
left join team_stats as aws
    on m.match_id = aws.match_id
    and m.away_team_id = aws.team_id
