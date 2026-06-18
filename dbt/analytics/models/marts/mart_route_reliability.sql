-- Produces the final route-level static reliability score consumed by the API.

{{ 
    config(
        materialized='table',
        cluster_by=bigquery_config(["route_id"]),
        indexes=postgres_indexes([{'columns': ['route_id']}])
    ) 
}}

with base as (

    select
        stop_connectivity.route_id,
        stop_connectivity.avg_stop_connectivity,
        frequency_summary.avg_trips_per_hour,
        frequency_summary.min_trips_per_hour,
        transfer_risk.avg_transfer_risk,
        headway_stability.headway_variability_ratio
    from {{ ref('int_route_stop_connectivity') }} stop_connectivity  -- ~ distinct routes per stop  
    join {{ ref('int_route_frequency_summary') }} frequency_summary  -- ~ service events per time interval
        on stop_connectivity.route_id = frequency_summary.route_id
    left join {{ ref('int_route_transfer_risk') }} transfer_risk  -- ~ scheduled platform-to-platform transfer risks
        on stop_connectivity.route_id = transfer_risk.route_id
    left join {{ ref('int_route_headway_stability') }} headway_stability  -- ~ how evenly spaced is the service
        on stop_connectivity.route_id = headway_stability.route_id

)

select
    b.route_id,
    b.avg_stop_connectivity,
    b.avg_trips_per_hour,
    b.min_trips_per_hour,
    b.avg_transfer_risk,
    b.headway_variability_ratio,

    (
        0.25 * b.avg_stop_connectivity +
        0.125 * b.avg_trips_per_hour +
        0.125 * b.min_trips_per_hour +
        0.25 * (1 - coalesce(b.avg_transfer_risk, 0.5)) +
        0.25 * (1 / (1 + coalesce(b.headway_variability_ratio, 1)))
    ) * 100 as reliability_score

from base b
