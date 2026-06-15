-- Summarizes hourly route frequency into average, peak, and minimum service levels.

select
    route_id,
    avg(trips_per_hour) as avg_trips_per_hour,
    max(trips_per_hour) as peak_trips_per_hour,
    min(trips_per_hour) as min_trips_per_hour
from {{ ref('int_route_frequency') }}
group by 1
