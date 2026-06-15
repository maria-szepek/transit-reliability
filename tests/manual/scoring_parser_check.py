# Manual smoke check for OTP route ID extraction from fetched itineraries.

from services.scoring.otp_client import extract_route_ids, get_itineraries


routes = get_itineraries(
    40.7128,
    -74.0060,
    40.7580,
    -73.9855,
)

for route in routes:
    print(extract_route_ids(route))
