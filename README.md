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
## Deployment (Full Stack)
### 1. Clone repository
```
git clone 
cd transit-reliability
```
### 2. Create environment file

Create .env:
```
POSTGRES_HOST=postgres
POSTGRES_USER=transit
POSTGRES_PASSWORD=transit
POSTGRES_DB=transit
OTP_URL=http://otp:8080/otp/routers/default/plan
```
### 3. Start stack
```
docker compose up --build
```

### 4. Load GTFS data

Run ingestion container:
```
docker compose run ingestion
```
This loads GTFS files into Postgres.

### 5. Run dbt models
```
cd dbt/analytics
uv run dbt run
```

This builds reliability feature tables.

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

* No GTFS realtime ingestion
* No actual delay modeling
* Transfer risk is approximate
* API requires latitude/longitude (no geocoding yet)
* Duplicate itineraries may appear
* No user preference weighting
* No CI/testing yet
* No cloud deployment

The system ranks structural reliability, not operational performance.

## Future Work

Planned improvements:

* GTFS realtime ingestion
* delay variance modeling
* transfer failure probability
* time-of-day reliability refinement
* address-based input (geocoding)
* Airflow orchestration
* Kafka streaming
* AWS deployment
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
* Airflow (planned)
* Kafka (planned)

## Status

Reliability modeling is partially implemented.
Realtime intelligence and orchestration remain future work.


# UPDATE SINCE LAST COMMIT: 

Done: 

1. created infra - terraform scripts for gcp deployment 
2. created makefile 

3. local development:

3.1. scoring engine taking into consideration the new realtime data processing tables  
3.2 update explanation layer 
3.3 update api: add geocoding / place resolution layer to allow API to accept either coordinates or text locations
3.4  write airflow dag(s) (static gtfs retrieval, osm updates, wahrehouse cleanup ) 
3.5 write streamlit app (it is not good but at least it exists)

Next steps: 
4. cloud deployment 

4.1. move the whole thing to the cloud infra 
4.2  rewire everything (bucket, hw, vm, etc etc ) 
4.3 host app somehow 

5. complete and test makefile
6. test thoroughly 

7. optional: allow local deployment mode
8. optional: write tests 



