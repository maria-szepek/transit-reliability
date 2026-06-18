-- A headway is the time gap between two consecutive vehicles on the same route.
-- int_route_headway_stability measures how evenly spaced service is for each route.
-- avg_headway_minutes tells us the typical service gap.
-- headway_stddev_minutes tells us how much those gaps vary.
-- headway_variability_ratio = stddev / average, so lower values mean more stable spacing.

{{ config(materialized='table') }}

with arrivals as (

    select
        t.route_id,
        st.stop_id,
        st.arrival_seconds
    from {{ ref('stg_stop_times') }} st
    join {{ ref('stg_trips') }} t using (trip_id)

),

ordered as (

    select
        *,
        lead(arrival_seconds) over (
            partition by route_id, stop_id
            order by arrival_seconds
        ) as next_arrival_seconds
    from arrivals

),

headways as (

    select
        route_id,
        (next_arrival_seconds - arrival_seconds) / 60.0 as headway_minutes
    from ordered
    where next_arrival_seconds is not null
      and (next_arrival_seconds - arrival_seconds) between 60 and 7200

),

route_headways as (

    select
        route_id,
        avg(headway_minutes) as avg_headway_minutes,
        stddev(headway_minutes) as headway_stddev_minutes
    from headways
    group by route_id

)

select
    route_id,
    avg_headway_minutes,
    headway_stddev_minutes,
    headway_stddev_minutes / nullif(avg_headway_minutes, 0) as headway_variability_ratio
from route_headways
