-- Counts how many distinct routes serve each stop as a proxy for rerouting options.

select
    st.stop_id,
    count(distinct t.route_id) as number_of_routes
from {{ ref('stg_stop_times') }} st
join {{ ref('stg_trips') }} t
    on st.trip_id = t.trip_id
group by 1
