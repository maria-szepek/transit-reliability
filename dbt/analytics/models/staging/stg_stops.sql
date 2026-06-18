-- Cleans GTFS stops while preserving station/platform hierarchy fields.

select
    stop_id,
    stop_name,
    stop_desc,
    {{ as_float('stop_lat') }} as stop_lat,
    {{ as_float('stop_lon') }} as stop_lon,
    zone_id,
    stop_url,
    {{ as_int('location_type') }} as location_type,
    parent_station,
    stop_code,
    {{ as_int('wheelchair_boarding') }} as wheelchair_boarding
from {{ source('raw', 'stops') }}
