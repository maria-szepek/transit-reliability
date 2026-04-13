from fastapi import FastAPI
from services.scoring.scorer import score_routes
from services.scoring.itinerary_formatter import format_itinerary

from services.api.geocoding import geocode

app = FastAPI()


@app.get("/routes/reliable")
def reliable_routes(
    from_lat: float | None = None,
    from_lon: float | None = None,
    to_lat: float | None = None,
    to_lon: float | None = None,
    from_place: str | None = None,
    to_place: str | None = None
):
    # geocoding resolve text locations if provided
    if from_place:
        from_lat, from_lon = geocode(from_place)

    if to_place:
        to_lat, to_lon = geocode(to_place)

    if None in (from_lat, from_lon, to_lat, to_lon):
        raise HTTPException(
            status_code=400,
            detail="Provide either coordinates or place names"
        )

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