-- Cleans GTFS stop_times and adds reusable parsed time fields in seconds and hours.

{{ 
  config(
    materialized='table',
    post_hook=[
      "create index if not exists idx_stg_stop_times_stop on {{ this }} (stop_id)",
      "create index if not exists idx_stg_stop_times_trip on {{ this }} (trip_id)",
      "create index if not exists idx_stg_stop_times_stop_time on {{ this }} (stop_id, arrival_time)"
    ]
  ) 
}}

    select
        trip_id,
        arrival_time,
        departure_time,
        (
            split_part(arrival_time, ':', 1)::int * 3600 +
            split_part(arrival_time, ':', 2)::int * 60 +
            split_part(arrival_time, ':', 3)::int
        ) as arrival_seconds,
        (
            split_part(departure_time, ':', 1)::int * 3600 +
            split_part(departure_time, ':', 2)::int * 60 +
            split_part(departure_time, ':', 3)::int
        ) as departure_seconds,
        split_part(arrival_time, ':', 1)::int as arrival_hour,
        split_part(departure_time, ':', 1)::int as departure_hour,
        stop_id,
    stop_sequence::int as stop_sequence,
    pickup_type::int as pickup_type,
    drop_off_type::int as drop_off_type,
    timepoint::int as timepoint,
    track,
    note_id
from {{ source('raw', 'stop_times') }}
