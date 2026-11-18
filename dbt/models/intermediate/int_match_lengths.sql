-- Length of each match in clock minutes (incl. stoppage & extra time, excl. shoot-out).
-- Used to resolve "on the pitch until the final whistle" into minutes played.
with events as (
    select * from {{ ref('stg_statsbomb__events') }}
)

select
    match_id,
    max(case when period < 5 then match_clock_minutes end)   as match_length_minutes,
    max(period)                                              as last_period,
    max(period) >= 3                                         as went_to_extra_time,
    max(period) = 5                                          as went_to_penalties
from events
group by match_id
