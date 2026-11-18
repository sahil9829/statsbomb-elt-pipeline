-- Grain: one row per stadium.
with stadiums as (
    select
        stadium_id,
        stadium_name,
        stadium_country_name,
        row_number() over (partition by stadium_id order by match_date desc) as rn
    from {{ ref('stg_statsbomb__matches') }}
    where stadium_id is not null
),

venues as (
    select stadium_id as venue_id, stadium_name as venue_name, stadium_country_name as venue_country_name
    from stadiums
    where rn = 1

    union all

    select cast({{ var('unknown_id') }} as bigint), 'Unknown', cast(null as {{ dbt.type_string() }})
)

select
    {{ dim_key(['venue_id']) }} as venue_key,
    venue_id,
    venue_name,
    venue_country_name,
    venue_id = {{ var('unknown_id') }} as is_unknown_member
from venues
