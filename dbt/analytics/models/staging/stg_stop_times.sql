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
    -- no casts due to 24:04:00 for example however i am supposed to interpret that
    arrival_time,
    departure_time,
    stop_id,
    stop_sequence::int as stop_sequence,
    pickup_type::int as pickup_type,
    drop_off_type::int as drop_off_type,
    timepoint::int as timepoint,
    track,
    note_id
from {{ source('raw', 'stop_times') }}