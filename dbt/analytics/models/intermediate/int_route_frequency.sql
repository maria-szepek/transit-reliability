-- "how often does each route run?"

-- route_id | hour_of_day | trips_per_hour

-- This feeds:

-- reroute resilience
-- time-of-day reliability
-- transfer risk

-- select
--     t.route_id,
--     extract(hour from st.departure_time) as hour_of_day,
--     count(*) as trips_per_hour
-- from {{ ref('stg_stop_times') }} st
-- join {{ ref('stg_trips') }} t
--     on st.trip_id = t.trip_id
-- group by 1,2

select
    t.route_id,
    -- (split_part(st.departure_time, ':', 1)::int % 24) as hour_of_day
    split_part(st.departure_time, ':', 1)::int as hour_of_day,
    count(*) as trips_per_hour
from {{ ref('stg_stop_times') }} st
join {{ ref('stg_trips') }} t
    on st.trip_id = t.trip_id
group by 1,2

-- TODO: (make_interval(hours := ...)) maybe? 