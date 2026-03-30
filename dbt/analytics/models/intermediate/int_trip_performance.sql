{{ config(materialized='table') }}

with stop_times as (

    select
        trip_id,
        stop_sequence,
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
        st.trip_id,
        st.stop_sequence,

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
            partition by trip_id
            order by stop_sequence
        ) as next_arrival
    from joined

),

segments as (

    select
        route_id,
        trip_id,
        arrival_seconds,
        (next_arrival - arrival_seconds) as travel_time_seconds
    from ordered
    where next_arrival is not null
      and (next_arrival - arrival_seconds) between 30 and 7200
),

route_avg as (

    select
        route_id,
        avg(travel_time_seconds) as route_avg_tt
    from segments
    group by route_id

),

bucketed as (

    select
        s.route_id,
        (s.arrival_seconds / 3600)::int as hour_of_day,
        s.travel_time_seconds,
        r.route_avg_tt
    from segments s
    join route_avg r
        on s.route_id = r.route_id
)

select
    route_id,
    hour_of_day,
    avg(travel_time_seconds) / 60 as avg_duration_min,
    stddev(travel_time_seconds) / 60 as std_duration_min,

    avg(
        case
            when travel_time_seconds > route_avg_tt * 1.25
            then 1 else 0
        end
    ) as delay_probability

from bucketed
group by route_id, hour_of_day