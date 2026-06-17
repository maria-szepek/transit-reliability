-- Prepares scheduled stop events for transfer-risk matching.

{{
  config(
    materialized='table',
    post_hook=[
      "create index if not exists idx_int_transfer_events_station_arrival on {{ this }} (station_id, arrival_seconds)",
      "create index if not exists idx_int_transfer_events_station_departure on {{ this }} (station_id, departure_seconds)",
      "create index if not exists idx_int_transfer_events_station_route_departure on {{ this }} (station_id, route_id, departure_seconds)",
      "create index if not exists idx_int_transfer_events_stop on {{ this }} (stop_id)",
      "analyze {{ this }}"
    ]
  )
}}

select
    t.route_id,
    st.trip_id,
    st.stop_id,
    coalesce(nullif(s.parent_station, ''), st.stop_id) as station_id,
    st.arrival_seconds,
    st.departure_seconds
from {{ ref('stg_stop_times') }} st
join {{ ref('stg_trips') }} t using (trip_id)
left join {{ ref('stg_stops') }} s using (stop_id)
