{{ config(materialized='table') }}

with trip_delay as (

    select
        trip_id,
        route_id,
        delay_risk
    from {{ ref('int_realtime_trip_delay') }}

),

route_agg as (

    select
        route_id,
        avg(delay_risk) as realtime_delay_risk,
        count(*) as active_trips
    from trip_delay
    group by route_id

)

select
    route_id,
    realtime_delay_risk,
    active_trips
from route_agg