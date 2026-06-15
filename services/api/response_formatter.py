# Formats scored OTP itineraries into the JSON response returned by the FastAPI route.

def build_route_response(scored_routes):
    if not scored_routes:
        return []

    response = []
    seen = set()
    rank = 1

    fastest_duration = min(
        round(route["itinerary"]["duration"] / 60)
        for route in scored_routes
    )

    for route in scored_routes:
        legs = format_transit_legs(route["itinerary"])
        key = route_identity(legs)

        if key in seen:
            continue

        seen.add(key)
        duration = round(route["itinerary"]["duration"] / 60)

        response.append({
            "rank": rank,
            "recommended": rank == 1,
            "fastest": duration == fastest_duration,
            "duration_min": duration,
            "transfers": route["itinerary"]["transfers"],
            "reliability_score": round(float(route["score"]), 2),
            "explanation": route.get("explanation"),
            "legs": legs,
        })

        rank += 1

    return response


def format_transit_legs(itinerary):
    legs_output = []

    for leg in itinerary["legs"]:
        if not leg.get("transitLeg"):
            continue

        legs_output.append({
            "line": leg.get("routeShortName"),
            "mode": leg.get("mode"),
            "from": leg["from"]["name"],
            "to": leg["to"]["name"],
        })

    return legs_output


def route_identity(legs):
    return tuple(
        (leg["line"], leg["from"], leg["to"])
        for leg in legs
    )
