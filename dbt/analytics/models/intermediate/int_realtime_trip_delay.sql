{{ config(materialized='table') }}

with recent as (

    select
        trip_id,
        route_id,
        ingestion_time
    from {{ ref('stg_realtime_trip_updates') }}
    where ingestion_time >= now() - interval '30 minutes'

),

trip_counts as (

    select
        trip_id,
        route_id,
        count(*) as update_count
    from recent
    group by 1,2

)

select
    trip_id,
    route_id,
    update_count,

    case
        when update_count >= 20 then 0.9
        when update_count >= 10 then 0.7
        when update_count >= 5 then 0.5
        else 0.2
    end as delay_risk

from trip_counts