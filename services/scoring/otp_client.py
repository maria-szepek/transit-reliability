import requests
import os


# OTP_URL = "http://localhost:8088/otp/routers/default/plan"
# OTP_URL = "http://otp:8080/otp/routers/default/plan"
OTP_URL = os.getenv(
    "OTP_URL",
    "http://localhost:8088/otp/routers/default/plan"
)


def get_routes(from_lat, from_lon, to_lat, to_lon):
    params = {
        "fromPlace": f"{from_lat},{from_lon}",
        "toPlace": f"{to_lat},{to_lon}",
        "mode": "TRANSIT,WALK",
        "numItineraries": 5
    }

    response = requests.get(OTP_URL, params=params)
    response.raise_for_status()

    return response.json()["plan"]["itineraries"]