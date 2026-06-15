-- Calculates the average stop connectivity for each route based on routes available at its stops.

select
    t.route_id,
    avg(sc.number_of_routes) as avg_stop_connectivity
from {{ ref('stg_stop_times') }} st
join {{ ref('stg_trips') }} t
    on st.trip_id = t.trip_id
join {{ ref('int_stop_connectivity') }} sc
    on st.stop_id = sc.stop_id
group by 1
