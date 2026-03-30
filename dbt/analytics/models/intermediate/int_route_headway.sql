-- schedule stability per route

{{ config(materialized='table') }}

with stop_times as (

    select
        trip_id,
        stop_id,
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
        st.stop_id,

        (
            split_part(st.arrival_time, ':', 1)::int * 3600 +
            split_part(st.arrival_time, ':', 2)::int * 60 +
            split_part(st.arrival_time, ':', 3)::int
        ) as arrival_seconds

    from stop_times st
    join trips t using (trip_id)

),

ordered as (

    select
        *,
        lead(arrival_seconds) over (
            partition by route_id, stop_id
            order by arrival_seconds
        ) as next_arrival

    from joined

),

headways as (

    select
        route_id,
        (next_arrival - arrival_seconds)/60.0 as headway_min
    from ordered
    where next_arrival is not null
      and (next_arrival - arrival_seconds) between 60 and 7200
)

select
    route_id,
    avg(headway_min) as avg_headway_min,
    stddev(headway_min) as headway_stddev
from headways
group by route_id