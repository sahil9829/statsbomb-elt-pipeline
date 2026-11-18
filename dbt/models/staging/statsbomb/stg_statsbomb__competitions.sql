with source as (
    select * from {{ source('statsbomb', 'competitions') }}
)

select
    competition_id,
    season_id,
    competition_name,
    country_name                                        as competition_country_name,
    competition_gender,
    competition_youth                                   as is_youth,
    competition_international                           as is_international,
    season_name,
    -- '2018' -> 2018, '2015/2016' -> 2015
    cast(substr(season_name, 1, 4) as integer)          as season_start_year,
    cast(match_updated_at as timestamp)                 as match_updated_at,
    cast(_ingested_at as timestamp)                     as _ingested_at
from source
