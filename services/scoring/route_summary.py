def summarize_itinerary(itinerary):
    transit_legs = [
        leg for leg in itinerary["legs"]
        if leg.get("transitLeg")
    ]

    if not transit_legs:
        return None

    first = transit_legs[0]

    return {
        "line": first.get("routeShortName"),
        "mode": first.get("mode"),
    }