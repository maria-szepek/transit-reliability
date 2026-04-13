{{ config(materialized='table') }}

with base as (

    select
        route_id,
        avg_stop_connectivity,
        avg_trips_per_hour,
        min_trips_per_hour,
        avg_transfer_risk,
        headway_stddev,
        delay_variance,
        time_reliability,
        -- realtime_delay_risk,
        reliability_score
    from {{ ref('mart_route_reliability') }}

)

select
    route_id,
    reliability_score,

    concat_ws(
        ', ',

        case 
            when avg_stop_connectivity > 5 
            then 'many reroute options' 
        end,

        case 
            when avg_trips_per_hour > 6 
            then 'high frequency' 
        end,

        case 
            when min_trips_per_hour > 2 
            then 'low worst-case wait' 
        end,

        case 
            when avg_transfer_risk < 0.4 
            then 'low transfer risk' 
        end,

        case 
            when headway_stddev < 4 
            then 'stable headways' 
        end,

        case 
            when time_reliability > 0.7 
            then 'historically on-time' 
        end,

        case 
            when delay_variance < 3 
            then 'low travel time variability' 
        end

        -- case 
        --     when realtime_delay_risk > 0.6 
        --     then 'realtime delays detected' 
        -- end

    ) as explanation

from base