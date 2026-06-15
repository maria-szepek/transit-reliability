# Scores OTP itineraries by combining static dbt reliability scores with realtime route risk.
#
# Flow:
#   -> fetches OTP itineraries from otp_client -> extracts route IDs
#   -> loads static route scores from Postgres
#   -> loads realtime route risks from Postgres
#   -> combines static + realtime signals
#   -> ranks itineraries

import os
from contextlib import closing

import psycopg2

from services.scoring.otp_client import extract_route_ids, get_itineraries


def connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        database=os.getenv("POSTGRES_DB", "transit"),
        user=os.getenv("POSTGRES_USER", "transit"),
        password=os.getenv("POSTGRES_PASSWORD", "transit"),
    )

#  -> loads static route scores from Postgres
def get_static_route_scores(route_ids):
    if not route_ids:
        return {}

    with closing(connect()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    r.route_id,
                    r.reliability_score,
                    e.explanation
                FROM analytics.mart_route_reliability r
                LEFT JOIN analytics.mart_route_explanations e
                    ON r.route_id = e.route_id
                WHERE r.route_id = ANY(%s)
                """,
                (list(route_ids),),
            )
            rows = cursor.fetchall()

    return {
        route_id: {
            "score": score,
            "explanation": explanation,
        }
        for route_id, score, explanation in rows
    }

# -> loads realtime route risks from Postgres
def get_realtime_route_risks(route_ids):
    if not route_ids:
        return {}

    with closing(connect()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (route_id)
                    route_id,
                    avg_abs_prediction_drift_seconds,
                    stddev_prediction_drift_seconds,
                    update_count,
                    stop_count
                FROM analytics.realtime_route_reliability
                WHERE route_id = ANY(%s)
                ORDER BY route_id, window_end DESC
                """,
                (list(route_ids),),
            )
            rows = cursor.fetchall()

    results = {}

    for route_id, avg_prediction_drift, prediction_volatility, update_count, stop_count in rows:
        drift_score = min(avg_prediction_drift / 300.0, 1.0)
        volatility_score = min(prediction_volatility / 180.0, 1.0)
        risk = 0.6 * drift_score + 0.4 * volatility_score

        results[route_id] = {
            "risk": risk,
            "explanation": (
                "unstable realtime predictions"
                if risk > 0.6 and update_count > 0 and stop_count > 0
                else None
            ),
        }

    return results


def score_routes(from_lat, from_lon, to_lat, to_lon):
    itineraries = get_itineraries(from_lat, from_lon, to_lat, to_lon)

    itinerary_route_ids = [
        extract_route_ids(itinerary)
        for itinerary in itineraries
    ]
    all_route_ids = sorted({
        route_id
        for route_ids in itinerary_route_ids
        for route_id in route_ids
    })

    static_scores = get_static_route_scores(all_route_ids)
    realtime_risks = get_realtime_route_risks(all_route_ids)

    scored = []

    for itinerary, route_ids in zip(itineraries, itinerary_route_ids):
        route_final_scores = []
        explanations = []

        for route_id in route_ids:
            static_entry = static_scores.get(route_id, {})
            realtime_entry = realtime_risks.get(route_id, {})

            static_score = static_entry.get("score")
            static_explanation = static_entry.get("explanation")

            realtime_risk = realtime_entry.get("risk", 0.0)
            realtime_explanation = realtime_entry.get("explanation")

            if static_score is None:
                static_score = 50.0

            final_route_score = float(static_score) * (1 - 0.15 * float(realtime_risk))
            route_final_scores.append(final_route_score)

            if static_explanation:
                explanations.append(f"{route_id}: {static_explanation}")

            if realtime_explanation:
                explanations.append(f"{route_id}: {realtime_explanation}")

        total_score = (
            sum(route_final_scores) / len(route_final_scores)
            if route_final_scores else 0.0
        )

        explanation = ", ".join(explanations) if explanations else None

        scored.append({
            "itinerary": itinerary,
            "score": total_score,
            "explanation": explanation,
        })

    return sorted(scored, key=lambda x: x["score"], reverse=True)
