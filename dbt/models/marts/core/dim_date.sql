with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('dim_date_start') ~ "' as date)",
        end_date="cast('" ~ var('dim_date_end') ~ "' as date)"
    ) }}
),

days as (
    select cast(date_day as date) as date_day from spine
),

enriched as (
    select
        date_day,
        cast(extract(year from date_day) as integer)        as year,
        cast(extract(quarter from date_day) as integer)     as quarter,
        cast(extract(month from date_day) as integer)       as month,
        cast(extract(day from date_day) as integer)         as day_of_month,
        cast({{ iso_day_of_week('date_day') }} as integer)  as day_of_week
    from days
)

select
    {{ date_key('date_day') }}                          as date_key,
    date_day                                            as full_date,
    year,
    quarter,
    month,
    case month
        when 1 then 'January' when 2 then 'February' when 3 then 'March'
        when 4 then 'April' when 5 then 'May' when 6 then 'June'
        when 7 then 'July' when 8 then 'August' when 9 then 'September'
        when 10 then 'October' when 11 then 'November' else 'December'
    end                                                 as month_name,
    day_of_month,
    day_of_week,
    case day_of_week
        when 1 then 'Monday' when 2 then 'Tuesday' when 3 then 'Wednesday'
        when 4 then 'Thursday' when 5 then 'Friday' when 6 then 'Saturday'
        else 'Sunday'
    end                                                 as day_name,
    day_of_week in (6, 7)                               as is_weekend,
    -- European football seasons run Aug-May: 2018-09-01 belongs to '2018/2019'
    case when month >= 7 then year else year - 1 end    as football_season_start_year
from enriched
