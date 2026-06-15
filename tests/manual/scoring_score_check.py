# Manual smoke check for end-to-end route scoring output.

from services.scoring.scorer import score_routes


results = score_routes(
    40.7128,
    -74.0060,
    40.7580,
    -73.9855,
)

for result in results:
    print(float(result["score"]))
