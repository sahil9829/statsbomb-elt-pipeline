-- Two rows per match (one per side) so facts can look up the opponent of any team.
with matches as (
    select * from {{ ref('stg_statsbomb__matches') }}
)

select match_id, home_team_id as team_id, away_team_id as opponent_team_id, true as is_home
from matches
union all
select match_id, away_team_id as team_id, home_team_id as opponent_team_id, false as is_home
from matches
