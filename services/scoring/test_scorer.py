from scorer import score_routes

results = score_routes(
    40.7128, -74.0060,
    40.7580, -73.9855
)

# for r in results:
#     print(r)

for r in results:
    print(float(r["score"]))