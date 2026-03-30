-- how many routes serve each stop

-- This is core to original reliability idea:

-- "will I be stuck"
-- "how many alternatives nearby"
-- "how easy to reroute"


select
    st.stop_id,
    count(distinct t.route_id) as number_of_routes
from {{ ref('stg_stop_times') }} st
join {{ ref('stg_trips') }} t
    on st.trip_id = t.trip_id
group by 1