import requests

def geocode(place: str):
    url = "https://nominatim.openstreetmap.org/search"

    params = {
        "q": place,
        "format": "json",
        "limit": 1
    }

    headers = {
        "User-Agent": "transit-reliability-app"
    }

    response = requests.get(url, params=params, headers=headers, timeout=10)
    response.raise_for_status()

    data = response.json()

    if not data:
        raise ValueError(f"Location not found: {place}")

    return float(data[0]["lat"]), float(data[0]["lon"])