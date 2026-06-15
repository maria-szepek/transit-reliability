# OpenTripPlanner client used by scoring to fetch itineraries and extract transit route IDs.

import os

import requests

OTP_URL = os.getenv(
    "OTP_URL",
    "http://localhost:8088/otp/routers/default/plan",
)


def get_itineraries(from_lat, from_lon, to_lat, to_lon):
    params = {
        "fromPlace": f"{from_lat},{from_lon}",
        "toPlace": f"{to_lat},{to_lon}",
        "mode": "TRANSIT,WALK",
        "numItineraries": 5,
    }

    response = requests.get(OTP_URL, params=params, timeout=15)
    response.raise_for_status()

    data = response.json()
    return data.get("plan", {}).get("itineraries", [])


def extract_route_ids(itinerary):
    route_ids = []

    for leg in itinerary["legs"]:
        if "routeId" in leg:
            route = leg["routeId"].split(":")[-1]
            route_ids.append(route)

    return route_ids
