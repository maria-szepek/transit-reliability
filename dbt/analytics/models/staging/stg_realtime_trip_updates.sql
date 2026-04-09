select
    trip_id,
    route_id,
    start_date,
    feed_timestamp,
    ingestion_time
from {{ source('raw_realtime', 'trip_updates') }}