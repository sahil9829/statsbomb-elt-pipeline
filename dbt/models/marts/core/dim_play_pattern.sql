-- Grain: one row per play pattern (how the possession started).
with patterns as (
    select distinct play_pattern_id, play_pattern_name
    from {{ ref('stg_statsbomb__events') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['play_pattern_id']) }} as play_pattern_key,
    play_pattern_id,
    play_pattern_name,
    play_pattern_name not in ('Regular Play', 'From Counter', 'From Keeper', 'Other') as is_set_piece
from patterns
