# Manual smoke check that OpenTripPlanner returns itinerary legs.

from services.scoring.otp_client import get_itineraries


routes = get_itineraries(
    40.7128,
    -74.0060,
    40.7580,
    -73.9855,
)

print(routes[0]["legs"])
