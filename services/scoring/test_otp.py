from otp_client import get_routes

routes = get_routes(
    40.7128, -74.0060,   # NYC downtown
    40.7580, -73.9855    # Times Square
)

print(len(routes))
print(routes[0])