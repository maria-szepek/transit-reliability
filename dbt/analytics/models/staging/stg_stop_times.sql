-- Cleans GTFS stop_times and adds reusable parsed time fields in seconds and hours.

{{ 
  config(
    materialized='table',
    partition_by=bigquery_config({
      "field": "arrival_hour",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 48,
        "interval": 1
      }
    }),
    cluster_by=bigquery_config(["trip_id", "stop_id"]),
    post_hook=postgres_post_hooks([
      "create index if not exists idx_stg_stop_times_stop on {{ this }} (stop_id)",
      "create index if not exists idx_stg_stop_times_trip on {{ this }} (trip_id)",
      "create index if not exists idx_stg_stop_times_arrival_hour on {{ this }} (arrival_hour)",
      "create index if not exists idx_stg_stop_times_stop_time on {{ this }} (stop_id, arrival_time)"
    ])
  ) 
}}

    select
        trip_id,
        arrival_time,
        departure_time,
        (
            {{ as_int(time_part('arrival_time', 1)) }} * 3600 +
            {{ as_int(time_part('arrival_time', 2)) }} * 60 +
            {{ as_int(time_part('arrival_time', 3)) }}
        ) as arrival_seconds,
        (
            {{ as_int(time_part('departure_time', 1)) }} * 3600 +
            {{ as_int(time_part('departure_time', 2)) }} * 60 +
            {{ as_int(time_part('departure_time', 3)) }}
        ) as departure_seconds,
        {{ as_int(time_part('arrival_time', 1)) }} as arrival_hour,
        {{ as_int(time_part('departure_time', 1)) }} as departure_hour,
        stop_id,
    {{ as_int('stop_sequence') }} as stop_sequence,
    {{ as_int('pickup_type') }} as pickup_type,
    {{ as_int('drop_off_type') }} as drop_off_type,
    {{ as_int('timepoint') }} as timepoint,
    track,
    note_id
from {{ source('raw', 'stop_times') }}
