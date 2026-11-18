-- Every team must name exactly 11 starters in every match.
select match_id, team_id, count(*) as starters
from {{ ref('stg_statsbomb__lineups') }}
where is_starter
group by match_id, team_id
having count(*) <> 11
