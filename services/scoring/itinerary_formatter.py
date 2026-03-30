def format_itinerary(itinerary):
    legs_output = []

    for leg in itinerary["legs"]:
        # ignore walking legs for clarity (optional, we can add later)
        if not leg.get("transitLeg"):
            continue

        legs_output.append({
            "line": leg.get("routeShortName"),
            "mode": leg.get("mode"),
            "from": leg["from"]["name"],
            "to": leg["to"]["name"]
        })

    return legs_output