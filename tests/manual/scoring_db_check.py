# Manual smoke check that static route reliability scores can be read from Postgres.

from services.scoring.scorer import get_static_route_scores


print(get_static_route_scores(["N", "R", "Q"]))
