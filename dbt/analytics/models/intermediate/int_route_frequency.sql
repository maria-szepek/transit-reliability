select
    t.route_id,
    split_part(st.departure_time, ':', 1)::int as hour_of_day,
    count(*) as trips_per_hour
from {{ ref('stg_stop_times') }} st
join {{ ref('stg_trips') }} t
    on st.trip_id = t.trip_id
group by 1,2
