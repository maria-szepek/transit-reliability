# FastAPI service exposing route planning, health checks, and database readiness endpoints.

import os
from contextlib import closing

import psycopg2
import requests
from fastapi import FastAPI, HTTPException
from services.api.response_formatter import build_route_response
from services.scoring.scorer import score_routes
from services.scoring.otp_client import OTP_URL

from services.api.geocoding import geocode

app = FastAPI()


def resolve_place(place: str, label: str) -> tuple[float, float]:
    try:
        return geocode(place)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Could not geocode {label} location",
        ) from exc


def check_postgres() -> None:
    with closing(psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        database=os.getenv("POSTGRES_DB", "transit"),
        user=os.getenv("POSTGRES_USER", "transit"),
        password=os.getenv("POSTGRES_PASSWORD", "transit"),
    )) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")


def check_otp() -> None:
    response = requests.get(OTP_URL, timeout=3)
    if response.status_code >= 500:
        response.raise_for_status()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ready")
def ready():
    checks = {}

    try:
        check_postgres()
        checks["postgres"] = "ok"
    except Exception as exc:
        checks["postgres"] = "unavailable"
        raise HTTPException(
            status_code=503,
            detail={"status": "not_ready", "checks": checks},
        ) from exc

    try:
        check_otp()
        checks["otp"] = "ok"
    except requests.RequestException as exc:
        checks["otp"] = "unavailable"
        raise HTTPException(
            status_code=503,
            detail={"status": "not_ready", "checks": checks},
        ) from exc

    return {"status": "ready", "checks": checks}


@app.get("/routes/reliable")
def reliable_routes(
    from_lat: float | None = None,
    from_lon: float | None = None,
    to_lat: float | None = None,
    to_lon: float | None = None,
    from_place: str | None = None,
    to_place: str | None = None
):
    if from_place:
        from_lat, from_lon = resolve_place(from_place, "origin")

    if to_place:
        to_lat, to_lon = resolve_place(to_place, "destination")

    if None in (from_lat, from_lon, to_lat, to_lon):
        raise HTTPException(
            status_code=400,
            detail="Provide either coordinates or place names"
        )

    try:
        results = score_routes(from_lat, from_lon, to_lat, to_lon)
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=502,
            detail="Could not retrieve routes from OpenTripPlanner",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Could not score routes",
        ) from exc

    return build_route_response(results)
