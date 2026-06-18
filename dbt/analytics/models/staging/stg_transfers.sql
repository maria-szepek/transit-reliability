-- Cleans GTFS transfer rules between stops, platforms, routes, and trips.

{{ 
  config(
    materialized='table',
    post_hook=postgres_post_hooks([
        "create index if not exists idx_stg_transfers_stop on {{ this }} (from_stop_id, to_stop_id)"
    ])
  ) 
}}


select
    from_stop_id,
    to_stop_id,
    {{ as_int('transfer_type') }} as transfer_type,
    {{ as_int('min_transfer_time') }} as min_transfer_time,
    from_route_id,
    to_route_id,
    from_trip_id,
    to_trip_id
from {{ source('raw', 'transfers') }}
