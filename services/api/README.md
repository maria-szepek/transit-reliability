# API Service

The API is the boundary between user-facing requests and route scoring.

## Request Flow

```text
UI
  -> GET /routes/reliable
  -> main.reliable_routes()
  -> geocoding.geocode() when place names are supplied
  -> scoring.scorer.score_routes()
  -> response_formatter.build_route_response()
  -> JSON response for the UI
```

## Responsibilities

- `main.py`: HTTP endpoints, input validation, dependency health checks, error mapping.
- `geocoding.py`: Converts place names to coordinates.
- `response_formatter.py`: Converts scored itineraries into the API response shape.

The scoring package owns ranking logic. The API package owns request/response shape.
