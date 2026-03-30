{{ config(materialized='table') }}

with base as (

    select
        route_id,
        hour_of_day,
        avg_duration_min,
        std_duration_min,
        delay_probability
    from {{ ref('int_trip_performance') }}

),

bucketed as (

    select
        route_id,

        case
            when hour_of_day between 6 and 9 then 'morning_peak'
            when hour_of_day between 16 and 19 then 'evening_peak'
            when hour_of_day between 10 and 15 then 'midday'
            else 'off_peak'
        end as time_bucket,

        avg_duration_min,
        std_duration_min,
        delay_probability

    from base
)

select
    route_id,
    time_bucket,
    avg(avg_duration_min) as avg_duration_min,
    avg(std_duration_min) as std_duration_min,
    avg(delay_probability) as delay_probability
from bucketed
group by route_id, time_bucket