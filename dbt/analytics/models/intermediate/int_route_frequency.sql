-- Counts scheduled trip starts per route and hour using each trip's first stop.

with first_stop_per_trip as (

    select
        trip_id,
        min(stop_sequence) as first_stop_sequence
    from {{ ref('stg_stop_times') }}
    group by trip_id

),

trip_starts as (

    select
        t.route_id,
        st.trip_id,
        st.departure_hour
    from {{ ref('stg_stop_times') }} st
    join first_stop_per_trip first_stop
        on st.trip_id = first_stop.trip_id
       and st.stop_sequence = first_stop.first_stop_sequence
    join {{ ref('stg_trips') }} t
        on st.trip_id = t.trip_id

)

select
    route_id,
    departure_hour as hour_of_day,
    count(*) as trips_per_hour
from trip_starts
group by 1, 2
