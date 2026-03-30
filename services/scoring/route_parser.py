# def extract_route_ids(itinerary):
#     route_ids = []

#     for leg in itinerary["legs"]:
#         if "routeId" in leg:
#             route_ids.append(leg["routeId"])

#     return route_ids


def extract_route_ids(itinerary):
    route_ids = []

    for leg in itinerary["legs"]:
        if "routeId" in leg:
            route = leg["routeId"].split(":")[-1]
            route_ids.append(route)

    return route_ids