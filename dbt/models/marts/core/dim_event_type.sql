-- Grain: one row per StatsBomb event type, with an analyst-friendly category.
with event_types as (
    select distinct event_type_id, event_type_name
    from {{ ref('stg_statsbomb__events') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['event_type_id']) }} as event_type_key,
    event_type_id,
    event_type_name,
    case
        when event_type_name in ('Shot', 'Own Goal For', 'Own Goal Against') then 'Shooting'
        when event_type_name in ('Pass', 'Ball Receipt*', 'Carry', 'Dribble', 'Miscontrol',
                                 'Dispossessed', 'Shield', 'Offside', 'Error') then 'Possession'
        when event_type_name in ('Pressure', 'Duel', 'Interception', 'Block', 'Clearance',
                                 'Ball Recovery', 'Dribbled Past', '50/50') then 'Defending'
        when event_type_name in ('Foul Committed', 'Foul Won', 'Bad Behaviour') then 'Discipline'
        when event_type_name = 'Goal Keeper' then 'Goalkeeping'
        else 'Match Admin'
    end as event_category,
    event_type_name in ('Pass', 'Carry', 'Dribble', 'Shot', 'Ball Receipt*', 'Miscontrol',
                        'Clearance', 'Interception', 'Ball Recovery', 'Block', 'Shield',
                        'Goal Keeper', '50/50') as is_on_ball
from event_types
