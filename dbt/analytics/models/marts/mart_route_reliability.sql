{{ 
    config(
        materialized='table',
        indexes=[{'columns': ['route_id']}]
    ) 
}}

with base as (

    select
        r.route_id,
        r.avg_stop_connectivity,
        f.avg_trips_per_hour,
        f.min_trips_per_hour,
        tr.avg_transfer_risk,
        h.headway_stddev
    from {{ ref('int_route_resilience') }} r
    join {{ ref('int_route_service_frequency') }} f
        on r.route_id = f.route_id
    left join {{ ref('int_route_transfer_risk') }} tr
        on r.route_id = tr.route_id
    left join {{ ref('int_route_headway') }} h
        on r.route_id = h.route_id

),

delay as (

    select
        route_id,
        avg(std_duration_min) as delay_variance
    from {{ ref('int_trip_performance') }}
    group by route_id

),

time_rel as (

    select
        route_id,
        avg(1 - delay_probability) as time_reliability
    from {{ ref('int_time_reliability') }}
    group by route_id

),

realtime as (

    select
        route_id,
        realtime_delay_risk
    from {{ ref('int_route_realtime_risk') }}

)

select
    b.route_id,
    b.avg_stop_connectivity,
    b.avg_trips_per_hour,
    b.min_trips_per_hour,
    b.avg_transfer_risk,
    b.headway_stddev,
    d.delay_variance,
    t.time_reliability,
    rt.realtime_delay_risk,

    (
        0.18 * b.avg_stop_connectivity +
        0.18 * b.avg_trips_per_hour +
        0.08 * b.min_trips_per_hour +
        0.12 * (1 - coalesce(b.avg_transfer_risk, 0.5)) +
        0.08 * (1 / (1 + coalesce(b.headway_stddev, 5))) +
        0.14 * coalesce(t.time_reliability, 0.5) +
        0.10 * (1 / (1 + coalesce(d.delay_variance, 5))) +
        0.12 * (1 - coalesce(rt.realtime_delay_risk, 0.5))
    ) * 100 as reliability_score

from base b
left join delay d using (route_id)
left join time_rel t using (route_id)
left join realtime rt using (route_id)