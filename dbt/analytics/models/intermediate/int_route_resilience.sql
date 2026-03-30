-- low value - route passes isolated stops - risky
-- high value - route passes hubs - resilient
-- ATTENTION it is not evident if this a robust way to measure resilience, because it doesnt help if in AVG it is resilent if i get stuck in the wrong station!! 
-- reevaluate 

select
    t.route_id,
    avg(sc.number_of_routes) as avg_stop_connectivity
from {{ ref('stg_stop_times') }} st
join {{ ref('stg_trips') }} t
    on st.trip_id = t.trip_id
join {{ ref('int_stop_connectivity') }} sc
    on st.stop_id = sc.stop_id
group by 1