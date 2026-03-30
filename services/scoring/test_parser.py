from otp_client import get_routes
from route_parser import extract_route_ids

routes = get_routes(
    40.7128, -74.0060,
    40.7580, -73.9855
)

for r in routes:
    print(extract_route_ids(r))