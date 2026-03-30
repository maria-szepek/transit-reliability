import psycopg2
import os


def get_route_scores(route_ids):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        database=os.getenv("POSTGRES_DB", "transit"),
        user=os.getenv("POSTGRES_USER", "transit"),
        password=os.getenv("POSTGRES_PASSWORD", "transit")
    )

    cursor = conn.cursor()

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

    cursor.execute(query, (route_ids,))
    results = cursor.fetchall()

    conn.close()

    return {
        route_id: {
            "score": score,
            "explanation": explanation
        }
        for route_id, score, explanation in results
    }