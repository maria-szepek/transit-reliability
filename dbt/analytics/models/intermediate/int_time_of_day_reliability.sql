{{ config(materialized='table') }}

with stop_times as (

    select
        trip_id,
        arrival_time
    from {{ ref('stg_stop_times') }}

),

trips as (

    select
        trip_id,
        route_id
    from {{ ref('stg_trips') }}

),

joined as (

    select
        t.route_id,

        (
            split_part(st.arrival_time, ':', 1)::int
        ) as hour_of_day

    from stop_times st
    join trips t using (trip_id)

),

bucketed as (

    select
        route_id,
        case
            when hour_of_day between 6 and 9 then 'morning_peak'
            when hour_of_day between 16 and 19 then 'evening_peak'
            when hour_of_day between 10 and 15 then 'midday'
            else 'off_peak'
        end as time_bucket
    from joined

)

select
    route_id,
    time_bucket,
    count(*) as trip_count
from bucketed
group by route_id, time_bucket