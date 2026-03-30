from fastapi import FastAPI
from services.scoring.scorer import score_routes
from services.scoring.itinerary_formatter import format_itinerary

app = FastAPI()


@app.get("/routes/reliable")
def reliable_routes(
    from_lat: float,
    from_lon: float,
    to_lat: float,
    to_lon: float
):
    results = score_routes(from_lat, from_lon, to_lat, to_lon)

    response = []
    seen = set()
    rank = 1

    # determine fastest duration first
    fastest_duration = min(
        round(r["itinerary"]["duration"] / 60)
        for r in results
    )

    for r in results:
        legs = format_itinerary(r["itinerary"])

        key = tuple(
            (leg["line"], leg["from"], leg["to"])
            for leg in legs
        )

        if key in seen:
            continue

        seen.add(key)

        duration = round(r["itinerary"]["duration"] / 60)

        response.append({
            "rank": rank,
            "recommended": rank == 1,
            "fastest": duration == fastest_duration,
            "duration_min": duration,
            "transfers": r["itinerary"]["transfers"],
            "reliability_score": round(float(r["score"]), 2),
            "explanation": r.get("explanation"),
            "legs": legs
        })

        rank += 1

    return response