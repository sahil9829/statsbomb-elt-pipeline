-- Grain: one row per player. Name/country from their latest appearance; primary position
-- is the position they started in most often.
with lineups as (
    select l.*, m.match_date
    from {{ ref('stg_statsbomb__lineups') }} as l
    inner join {{ ref('stg_statsbomb__matches') }} as m
        on l.match_id = m.match_id
),

latest as (
    select
        player_id,
        player_name,
        player_nickname,
        player_display_name,
        player_country_name,
        row_number() over (partition by player_id order by match_date desc, match_id desc) as rn
    from lineups
),

positions as (
    select
        player_id,
        first_position_name,
        row_number() over (
            partition by player_id
            order by count(*) desc, first_position_name
        ) as rn
    from lineups
    where appeared
    group by player_id, first_position_name
),

players as (
    select
        latest.player_id,
        latest.player_name,
        latest.player_nickname,
        latest.player_display_name,
        latest.player_country_name,
        positions.first_position_name as primary_position
    from latest
    left join positions
        on latest.player_id = positions.player_id
        and positions.rn = 1
    where latest.rn = 1

    union all

    select
        cast({{ var('unknown_id') }} as bigint),
        'Unknown',
        cast(null as {{ dbt.type_string() }}),
        'Unknown',
        cast(null as {{ dbt.type_string() }}),
        cast(null as {{ dbt.type_string() }})
)

select
    {{ dim_key(['player_id']) }} as player_key,
    player_id,
    player_name,
    player_nickname,
    player_display_name,
    player_country_name,
    primary_position,
    case
        when primary_position = 'Goalkeeper' then 'Goalkeeper'
        when primary_position like '%Back%' then 'Defender'
        when primary_position like '%Midfield%' then 'Midfielder'
        when primary_position like '%Forward%' or primary_position like '%Wing' then 'Forward'
    end as position_group,
    player_id = {{ var('unknown_id') }} as is_unknown_member
from players
