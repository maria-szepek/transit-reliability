import os
from contextlib import closing

import psycopg2


def get_realtime_scores(route_ids):
    if not route_ids:
        return {}

    with closing(psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        database=os.getenv("POSTGRES_DB", "transit"),
        user=os.getenv("POSTGRES_USER", "transit"),
        password=os.getenv("POSTGRES_PASSWORD", "transit"),
    )) as conn:
        with conn.cursor() as cursor:
            query = """
                SELECT DISTINCT ON (route_id)
                    route_id,
                    avg_abs_prediction_drift_seconds,
                    stddev_prediction_drift_seconds
                FROM analytics.realtime_stop_reliability
                WHERE route_id = ANY(%s)
                ORDER BY route_id, window_end DESC
            """

            cursor.execute(query, (list(route_ids),))
            rows = cursor.fetchall()

    results = {}

    for route_id, drift, volatility in rows:
        drift_score = min(drift / 300.0, 1.0)
        vol_score = min(volatility / 180.0, 1.0)

        risk = 0.6 * drift_score + 0.4 * vol_score

        results[route_id] = {
            "risk": risk,
            "explanation": "realtime delays detected" if risk > 0.6 else None
        }

    return results
