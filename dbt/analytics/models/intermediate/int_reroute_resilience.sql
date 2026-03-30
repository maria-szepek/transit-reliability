-- {{ config(materialized='table') }}

-- with stop_times as (

--     select trip_id, stop_id
--     from {{ ref('stg_stop_times') }}

-- ),

-- trips as (

--     select trip_id, route_id
--     from {{ ref('stg_trips') }}

-- ),

-- route_stops as (

--     select distinct
--         t.route_id,
--         st.stop_id
--     from stop_times st
--     join trips t using (trip_id)

-- ),

-- -- global intersections (simple metric)
-- global_intersections as (

--     select
--         r1.route_id,
--         count(distinct r2.route_id) as alternative_routes_global
--     from route_stops r1
--     join route_stops r2
--         on r1.stop_id = r2.stop_id
--        and r1.route_id != r2.route_id
--     group by r1.route_id

-- ),

-- -- per-stop connectivity (refined metric)
-- stop_connectivity as (

--     select
--         r1.route_id,
--         r1.stop_id,
--         count(distinct r2.route_id) - 1 as alternative_routes
--     from route_stops r1
--     join route_stops r2
--         on r1.stop_id = r2.stop_id
--     group by r1.route_id, r1.stop_id

-- ),

-- aggregated as (

--     select
--         route_id,
--         avg(alternative_routes) as avg_stop_connectivity,
--         max(alternative_routes) as max_connectivity,
--         count(*) filter (where alternative_routes > 2) as high_connectivity_stops
--     from stop_connectivity
--     group by route_id

-- )

-- select
--     a.route_id,
--     g.alternative_routes_global,
--     a.avg_stop_connectivity,
--     a.max_connectivity,
--     a.high_connectivity_stops

-- from aggregated a
-- join global_intersections g
--     on a.route_id = g.route_id


{{ config(materialized='table') }}

with stop_times as (

    select trip_id, stop_id
    from {{ ref('stg_stop_times') }}

),

trips as (

    select trip_id, route_id
    from {{ ref('stg_trips') }}

),

route_stops as (

    select distinct
        t.route_id,
        st.stop_id
    from stop_times st
    join trips t using (trip_id)

),

-- global intersections (simple metric)
global_intersections as (

    select
        r1.route_id,
        count(distinct r2.route_id) as alternative_routes_global
    from route_stops r1
    join route_stops r2
        on r1.stop_id = r2.stop_id
       and r1.route_id != r2.route_id
    group by r1.route_id

),

-- per-stop connectivity (refined metric)
stop_connectivity as (

    select
        r1.route_id,
        r1.stop_id,
        count(distinct r2.route_id) - 1 as alternative_routes
    from route_stops r1
    join route_stops r2
        on r1.stop_id = r2.stop_id
    group by r1.route_id, r1.stop_id

),

aggregated as (

    select
        route_id,
        avg(alternative_routes) as avg_stop_connectivity,
        max(alternative_routes) as max_connectivity,
        count(*) filter (where alternative_routes > 2) as high_connectivity_stops
    from stop_connectivity
    group by route_id

)

select
    a.route_id,
    g.alternative_routes_global,
    a.avg_stop_connectivity,
    a.max_connectivity,
    a.high_connectivity_stops

from aggregated a
join global_intersections g
    on a.route_id = g.route_id