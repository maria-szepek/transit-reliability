import os
from contextlib import closing

import psycopg2


def get_route_scores(route_ids):
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
                SELECT
                    r.route_id,
                    r.reliability_score,
                    e.explanation
                FROM analytics.mart_route_reliability r
                LEFT JOIN analytics.mart_route_explanations e
                    ON r.route_id = e.route_id
                WHERE r.route_id = ANY(%s)
            """

            cursor.execute(query, (list(route_ids),))
            results = cursor.fetchall()

    return {
        route_id: {
            "score": score,
            "explanation": explanation
        }
        for route_id, score, explanation in results
    }
