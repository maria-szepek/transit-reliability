# Transit Reliability Intelligence System

This project builds a prototype transit route reliability platform for the New York City region.
It combines OpenTripPlanner routing, GTFS static transit data, and dbt-based reliability feature modeling to rank transit routes based on structural reliability indicators.

The system is designed as a data engineering–focused architecture rather than a user-facing application.

## Project Scope

The goal is to evaluate transit routes not only by duration, but also by reliability proxies, including:

* service frequency
* reroute resilience
* transfer risk proxies
* headway stability
* structural connectivity

The system currently ranks routes using network structure and service density, not real-time delay data.

## Current Capabilities
### Routing
* OpenTripPlanner routing engine
* Multi-leg itinerary generation
* Transfer detection
* Duration extraction
### Reliability Modeling (dbt)
* route frequency modeling
* stop connectivity (reroute resilience proxy)
* transfer buffer approximation
* headway variability
* aggregated reliability score
### API
* FastAPI endpoint
* route ranking
* transfer-aware formatting
* reliability score per itinerary
### Containerization
* Docker Compose stack
* API container
* Postgres container
* OpenTripPlanner container
### Architecture
```
User
  ↓
FastAPI API
  ↓
Scoring Engine
  ↓
Postgres (dbt models)
  ↑
GTFS Static Data
  ↑
OpenTripPlanner (routing)
```
## Data Sources
### GTFS Static (Transit Data)


Download from:

https://data.ny.gov/Transportation/MTA-General-Transit-Feed-Specification-GTFS-Static/fgm6-ccue/about_data

Place the downloaded files into:
```
data/
```

### OpenStreetMap Data (for OpenTripPlanner)

Download New York extract:

https://download.geofabrik.de/north-america/us/new-york.html

Download:

new-york-latest.osm.pbf

Place into:
```
data/
```
## Deployment (Local)

### 1. Clone the repository

```bash
git clone <repository-url>
cd transit-reliability
```

### 2. Create the environment file

Create a `.env` file in the project root:

```env
POSTGRES_HOST=postgres
POSTGRES_USER=transit
POSTGRES_PASSWORD=transit
POSTGRES_DB=transit

OTP_URL=http://otp:8080/otp/routers/default/plan
```

### 3. Start the transit platform

From the project root:

```bash
docker compose up --build
```

This starts the core platform:

* Postgres
* Kafka
* OpenTripPlanner
* Flink
* FastAPI
* GTFS Realtime Producer

Wait until all services are running successfully.

### 4. Start Airflow

Open a new terminal and start the Airflow stack:

```bash
cd airflow
docker compose up --build
```

Open the Airflow UI:

```text
http://localhost:8080
```

### 5. Trigger the initial data pipelines

From the Airflow UI, manually trigger the following DAGs:

1. `osm_refresh`
2. `gtfs_static_refresh`
3. `warehouse_cleanup.py`

These workflows will:

* download the latest OpenStreetMap extract
* download the latest GTFS static feeds
* load GTFS data into Postgres
* run dbt models
* rebuild the OpenTripPlanner graph

Wait until both DAGs complete successfully.

### 6. Verify the platform

Open:

```text
API Docs:   http://localhost:8000/docs
Flink UI:   http://localhost:8081
OTP:        http://localhost:8088
Airflow:    http://localhost:8080
```

You should see:

* a running Flink job
* active GTFS realtime ingestion
* the `/routes/reliable` endpoint available in FastAPI

### 7. Start the Streamlit application

From the project root:

```bash
uv run streamlit run services/ui/app.py
```

### 8. Test the system

Open the Streamlit application or use the FastAPI Swagger UI:

```text
http://localhost:8000/docs
```

The `/routes/reliable` endpoint accepts either coordinates or text locations and returns ranked itineraries based on both static and realtime reliability metrics.


### 6. Test API

Example:

http://localhost:8000/routes/reliable?from_lat=40.7128&from_lon=-74.0060&to_lat=40.7580&to_lon=-73.9855

Example with transfer:

Test 2 — Astoria - Brooklyn (almost always 1 transfer and interesting example)
http://localhost:8000/routes/reliable?from_lat=40.7700&from_lon=-73.9180&to_lat=40.6500&to_lon=-73.9496
## Reliability Score

The current reliability score is based on:

* route frequency
* stop connectivity
* reroute resilience proxy
* headway stability
* transfer buffer proxy

These are structural indicators, not observed delays.

## Limitations

This project currently has several known limitations:

* Realtime scoring is based on GTFS-Realtime prediction drift, not observed arrival outcomes
* Transfer risk is approximate
* Duplicate itineraries may appear
* No user preference weighting
* Test coverage is limited
* Cloud deployment is scaffolded but not fully automated

The system is still a prototype and should be interpreted as reliability scoring by proxy.

## Future Work

Planned improvements:

* stronger delay variance modeling
* stronger transfer failure probability
* time-of-day reliability refinement
* user preference weighting
* cloud deployment hardening
* automated testing

## Example Output on http://localhost:8000/routes/reliable?from_lat=40.7700&from_lon=-73.9180&to_lat=40.6500&to_lon=-73.9496
```
[
  {
    "rank": 1,
    "recommended": true,
    "fastest": false,
    "duration_min": 71,
    "transfers": 2,
    "reliability_score": 215220.7,
    "explanation": "high frequency, low worst-case wait, reliable during this time of day, high frequency, low worst-case wait, stable headways, reliable during this time of day, stable travel time, high frequency, low worst-case wait, reliable during this time of day",
    "legs": [
      {
        "line": "W",
        "mode": "SUBWAY",
        "from": "Astoria Blvd",
        "to": "Lexington Av/59 St"
      },
      {
        "line": "4",
        "mode": "SUBWAY",
        "from": "59 St",
        "to": "Franklin Av-Medgar Evers College"
      },
      {
        "line": "2",
        "mode": "SUBWAY",
        "from": "Franklin Av-Medgar Evers College",
        "to": "Church Av"
      }
    ]
  },
  {
    "rank": 2,
    "recommended": false,
    "fastest": false,
    "duration_min": 69,
    "transfers": 1,
    "reliability_score": 79606.11,
    "explanation": "high frequency, low worst-case wait, reliable during this time of day, stable travel time, high frequency, low worst-case wait, reliable during this time of day",
    "legs": [
      {
        "line": "N",
        "mode": "SUBWAY",
        "from": "Astoria Blvd",
        "to": "Lexington Av/59 St"
      },
      {
        "line": "5",
        "mode": "SUBWAY",
        "from": "59 St",
        "to": "Church Av"
      }
    ]
  }
]
```

## Tech Stack
* Python
* FastAPI
* Postgres
* dbt
* OpenTripPlanner
* Docker Compose
* Airflow
* Kafka
* Flink
* Streamlit

## Status

Reliability modeling, realtime ingestion, Airflow orchestration, and the API/UI prototype are implemented for local development. Cloud deployment and production hardening remain incomplete.
