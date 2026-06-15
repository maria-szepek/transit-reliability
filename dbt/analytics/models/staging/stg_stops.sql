-- Cleans GTFS stops while preserving station/platform hierarchy fields.

select
    stop_id,
    stop_name,
    stop_desc,
    stop_lat::double precision as stop_lat,
    stop_lon::double precision as stop_lon,
    zone_id,
    stop_url,
    location_type::int         as location_type,
    parent_station,
    stop_code,
    wheelchair_boarding::int   as wheelchair_boarding
from {{ source('raw', 'stops') }}
