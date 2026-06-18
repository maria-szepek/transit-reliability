# Scoring Service

This package turns OpenTripPlanner itineraries into ranked reliability results.

## Runtime Flow

```text
API request
  -> scorer.score_routes()
  -> otp_client.get_itineraries()
  -> otp_client.extract_route_ids()
  -> scorer.get_static_route_scores()
  -> scorer.get_realtime_route_risks()
  -> ranked scored itineraries
```

## Files

- `scorer.py`: Loads score inputs from Postgres and ranks OTP itineraries.
- `otp_client.py`: Calls OpenTripPlanner and extracts route IDs from OTP itineraries.

API response formatting lives in `services/api/response_formatter.py`.
Manual smoke checks live under `tests/manual/`.
