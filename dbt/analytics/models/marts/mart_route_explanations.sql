-- Human-readable explanation labels for the same route-level signals used in
-- mart_route_reliability. This model should not introduce new scoring logic;
-- it only translates the reliability inputs into short positive reasons.

{{ config(materialized='table') }}

with base as (

    select
        route_id,
        avg_stop_connectivity,
        avg_trips_per_hour,
        min_trips_per_hour,
        avg_transfer_risk,
        headway_variability_ratio,
        reliability_score
    from {{ ref('mart_route_reliability') }}

)

select
    route_id,
    reliability_score,

    {{ join_non_null(
        ', ',
        [
            "case when avg_stop_connectivity > 5 then 'strong stop connectivity' end",
            "case when avg_trips_per_hour > 6 then 'frequent scheduled service' end",
            "case when min_trips_per_hour > 2 then 'strong minimum service level' end",
            "case when avg_transfer_risk < 0.4 then 'lower scheduled transfer risk' end",
            "case when headway_variability_ratio < 0.5 then 'more even vehicle spacing' end",
        ],
    ) }} as explanation

from base
