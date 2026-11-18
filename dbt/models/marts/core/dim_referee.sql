-- Grain: one row per referee.
with referees as (
    select
        referee_id,
        referee_name,
        referee_country_name,
        row_number() over (partition by referee_id order by match_date desc) as rn
    from {{ ref('stg_statsbomb__matches') }}
    where referee_id is not null
),

unioned as (
    select referee_id, referee_name, referee_country_name
    from referees
    where rn = 1

    union all

    select cast({{ var('unknown_id') }} as bigint), 'Unknown', cast(null as {{ dbt.type_string() }})
)

select
    {{ dim_key(['referee_id']) }} as referee_key,
    referee_id,
    referee_name,
    referee_country_name,
    referee_id = {{ var('unknown_id') }} as is_unknown_member
from unioned
