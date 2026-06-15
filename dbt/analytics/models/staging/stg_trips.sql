-- Cleans GTFS trips and keeps the route/service identifiers used by reliability models.

select
    route_id,
    service_id,
    trip_id,
    trip_headsign,
    direction_id::int       as direction_id,
    block_id,
    shape_id,
    trip_short_name,
    wheelchair_accessible::int as wheelchair_accessible,
    peak_offpeak
from {{ source('raw', 'trips') }}
