# Scores OTP itineraries by combining static dbt reliability scores with realtime route risk.
#
# Flow:
#   -> fetches OTP itineraries from otp_client -> extracts route IDs
#   -> loads static route scores from the selected warehouse
#   -> loads realtime route risks from Postgres
#   -> combines static + realtime signals
#   -> ranks itineraries

import os
from contextlib import closing

from google.cloud import bigquery
import psycopg2

from services.scoring.otp_client import extract_route_ids, get_itineraries

WAREHOUSE_BACKEND = os.getenv("WAREHOUSE_BACKEND", "postgres").lower()
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")


def connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        database=os.getenv("POSTGRES_DB", "transit"),
        user=os.getenv("POSTGRES_USER", "transit"),
        password=os.getenv("POSTGRES_PASSWORD", "transit"),
    )


def get_static_route_scores(route_ids):
    if WAREHOUSE_BACKEND == "bigquery":
        return get_static_route_scores_bigquery(route_ids)

    return get_static_route_scores_postgres(route_ids)


#  -> loads static route scores from Postgres
def get_static_route_scores_postgres(route_ids):
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


def get_static_route_scores_bigquery(route_ids):
    if not route_ids:
        return {}

    if not GCP_PROJECT_ID:
        raise ValueError("GCP_PROJECT_ID must be set when WAREHOUSE_BACKEND=bigquery")

    client = bigquery.Client(project=GCP_PROJECT_ID)
    query = f"""
        SELECT
            r.route_id,
            r.reliability_score,
            e.explanation
        FROM `{GCP_PROJECT_ID}.analytics.mart_route_reliability` r
        LEFT JOIN `{GCP_PROJECT_ID}.analytics.mart_route_explanations` e
            ON r.route_id = e.route_id
        WHERE r.route_id IN UNNEST(@route_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("route_ids", "STRING", list(route_ids))
        ]
    )
    rows = client.query(query, job_config=job_config).result()

    return {
        row.route_id: {
            "score": row.reliability_score,
            "explanation": row.explanation,
        }
        for row in rows
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
        drift_score = min(avg_prediction_drift / 300.0, 1.0) # >= 300s considered max risk
        volatility_score = min(prediction_volatility / 300.0, 1.0) # >= 300s considered max risk (180?)
        risk = 0.5 * drift_score + 0.5 * volatility_score

        results[route_id] = {
            "risk": risk,
            "explanation": (
                "unstable realtime predictions"
                if risk > 0.5 and update_count > 0 and stop_count > 0
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
                static_score = 0
                explanations.append(
                    f"{route_id}: no static data: assumed unreliable"
                )

            final_route_score = float(static_score) * (1 - 0.20 * float(realtime_risk))
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
