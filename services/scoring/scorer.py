from services.scoring.otp_client import get_routes
from services.scoring.route_parser import extract_route_ids
from services.scoring.reliability_lookup import get_route_scores
from services.scoring.realtime_lookup import get_realtime_scores


def score_routes(from_lat, from_lon, to_lat, to_lon):
    itineraries = get_routes(from_lat, from_lon, to_lat, to_lon)

    scored = []

    for itinerary in itineraries:
        route_ids = extract_route_ids(itinerary)

        static_scores = get_route_scores(route_ids)
        realtime_scores = get_realtime_scores(route_ids)

        route_final_scores = []
        explanations = []

        for route_id in route_ids:
            static_entry = static_scores.get(route_id, {})
            realtime_entry = realtime_scores.get(route_id, {})

            static_score = static_entry.get("score")
            static_explanation = static_entry.get("explanation")

            realtime_risk = realtime_entry.get("risk", 0.0)
            realtime_explanation = realtime_entry.get("explanation")

            # Fallback if no static score exists for this route
            if static_score is None:
                static_score = 50.0

            # Apply realtime penalty to static score
            # Example:
            #   risk = 0.0  -> no penalty
            #   risk = 1.0  -> 15% penalty
            # final_route_score = static_score * (1 - 0.15 * realtime_risk)
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