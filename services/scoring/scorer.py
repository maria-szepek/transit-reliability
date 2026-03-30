from services.scoring.otp_client import get_routes
from services.scoring.route_parser import extract_route_ids
from services.scoring.reliability_lookup import get_route_scores


def score_routes(from_lat, from_lon, to_lat, to_lon):
    itineraries = get_routes(from_lat, from_lon, to_lat, to_lon)

    scored = []

    for itinerary in itineraries:
        route_ids = extract_route_ids(itinerary)
        scores = get_route_scores(route_ids)

        # extract numeric scores
        numeric_scores = [
            s["score"] for s in scores.values()
            if s.get("score") is not None
        ]

        total_score = (
            sum(numeric_scores) / len(numeric_scores)
            if numeric_scores else 0
        )

        # collect explanations
        explanations = [
            s["explanation"] for s in scores.values()
            if s.get("explanation")
        ]

        explanation = ", ".join(explanations) if explanations else None

        scored.append({
            "itinerary": itinerary,
            "score": total_score,
            "explanation": explanation
        })

    return sorted(scored, key=lambda x: x["score"], reverse=True)