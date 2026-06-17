# Transit Reliability Intelligence System

This project builds a local transit reliability platform for the New York City region.

The idea is to rank transit itineraries not only by travel time, but also by reliability proxies built from GTFS static data, GTFS-Realtime feeds, OpenStreetMap, OpenTripPlanner, dbt models, and streaming metrics.

This is mostly a data engineering project. The UI exists so the pipeline can be tested end to end, but the main point is the backend/data architecture.

## Terminology

Route: A transit service or line, such as the A, Q, or N train. A route is the service passengers recognize.

Trip: A single scheduled run of a route. For example, the A train departing at 8:05 AM and the A train departing at 8:15 AM are two different trips on the same route.

Itinerary: A journey returned by OpenTripPlanner from an origin to a destination. An itinerary may contain one or more legs.

Leg: One segment of an itinerary on a specific route. For example, an itinerary may include an A train leg followed by a Q train leg.

Stop: A physical place where passengers board or leave a vehicle. In GTFS, stops may represent stations, platforms, or boarding locations depending on the feed.

## What the Project Does

The system currently combines:

* static GTFS schedule data
* OpenStreetMap data for OpenTripPlanner routing
* dbt models for route-level reliability features
* GTFS-Realtime trip updates streamed through Kafka and Flink
* FastAPI route scoring
* a lightweight Streamlit UI
* Airflow DAGs for local refresh/orchestration

The current reliability score is still a proxy. It is not yet a full observed delay model.

## Reliability Signals

The static dbt reliability score uses:

* scheduled trip frequency
* minimum service level by hour
* stop connectivity
* transfer risk proxy
* headway stability

The realtime layer currently measures:

* GTFS-Realtime prediction drift
* route-level prediction instability

Important: realtime scoring currently measures **prediction instability**, not actual schedule delay. GTFS-Realtime supports `arrival.delay` and `departure.delay`, but this project still needs feed validation before using those fields as delay metrics.

## Architecture

```text
Airflow
  -> downloads GTFS static feeds
  -> downloads OSM extract
  -> loads GTFS into Postgres
  -> runs dbt models
  -> rebuilds OpenTripPlanner graph locally

GTFS-Realtime Producer
  -> polls MTA GTFS-Realtime feeds
  -> publishes raw protobuf payloads to Kafka

Kafka
  -> gtfs.trip_updates topic

Flink
  -> consumes GTFS-Realtime messages
  -> decodes trip updates
  -> calculates prediction drift
  -> writes realtime stop and route reliability tables to Postgres

Postgres
  -> raw GTFS tables
  -> dbt analytics models
  -> realtime reliability tables

OpenTripPlanner
  -> builds routing graph from GTFS + OSM
  -> returns candidate itineraries

FastAPI
  -> receives route requests
  -> asks OpenTripPlanner for itineraries
  -> scores routes using static + realtime reliability signals

Streamlit
  -> simple route search and analytics UI
```

## Main Services

Postgres: application warehouse for raw GTFS, dbt models, and realtime reliability tables.

Airflow Postgres: separate metadata database for Airflow itself.

Kafka: Apache Kafka in KRaft mode. No ZooKeeper.

Flink: stream job that turns GTFS-Realtime trip updates into prediction-stability metrics.

OpenTripPlanner: routing engine.

FastAPI: route scoring API.

Streamlit: local UI, started separately with `uv`.

Airflow: local orchestration for GTFS/OSM refresh, dbt, cleanup, and OTP rebuild.

## Data Sources

GTFS Static:

https://data.ny.gov/Transportation/MTA-General-Transit-Feed-Specification-GTFS-Static/fgm6-ccue/about_data

OpenStreetMap New York extract:

https://download.geofabrik.de/north-america/us/new-york.html

GTFS-Realtime MTA feeds:

Used by `services/ingestion/realtime/gtfs_realtime_producer.py`.

## Local Deployment

### 1. Clone the repository

```bash
git clone <repository-url>
cd transit-reliability
```

### 2. Create `.env`

Create a `.env` file in the project root.

Example:

```env
POSTGRES_HOST=postgres
POSTGRES_USER=transit
POSTGRES_PASSWORD=transit
POSTGRES_DB=transit

OTP_URL=http://otp:8080/otp/routers/default/plan
API_URL=http://api:8000/routes/reliable

AIRFLOW_UID=50000
DOCKER_GID=999

PGADMIN_DEFAULT_EMAIL=admin@admin.com
PGADMIN_DEFAULT_PASSWORD=<generate-a-local-password>

AIRFLOW_POSTGRES_PASSWORD=<generate-a-local-password>
AIRFLOW__CORE__FERNET_KEY=<generate-with-airflow-fernet-key-or-python-cryptography>
AIRFLOW__API_AUTH__JWT_SECRET=<generate-a-random-secret>
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=<generate-a-local-password>
```

For local testing, the values can be simple. For anything public/cloud-facing, do not use default passwords.

### 3. Start the stack

```bash
docker compose up --build
```

The first build can take a while because Airflow and Flink base images are large.

This starts:

* Postgres
* PgAdmin
* Kafka
* GTFS-Realtime producer
* Flink JobManager/TaskManager/job submitter
* OpenTripPlanner
* FastAPI
* Airflow

### 4. Run the initial pipelines

Open Airflow:

```text
http://localhost:8080
```

Trigger:

1. `osm_refresh`
2. `gtfs_static_refresh`

`warehouse_cleanup` is a scheduled maintenance DAG. It does not need to be the first thing you run.

The refresh DAGs download data, load GTFS into Postgres, run dbt, and rebuild OTP locally.

### 5. Check services

```text
FastAPI docs: http://localhost:8000/docs
Flink UI:     http://localhost:8081
OTP:          http://localhost:8088
Airflow:      http://localhost:8080
PgAdmin:      http://localhost:8085
```

### 6. Start Streamlit

From the project root:

```bash
uv run --package transit-reliability-ui streamlit run services/ui/app.py
```

If you run Streamlit outside Docker, set local host values if needed:

```bash
POSTGRES_HOST=localhost API_URL=http://localhost:8000/routes/reliable uv run --package transit-reliability-ui streamlit run services/ui/app.py
```

## Dependency Management

This repository uses uv for Python dependency management.

Most Python code belongs to the root uv workspace:

```text
root pyproject.toml
root uv.lock
  -> dbt
  -> services/api
  -> services/scoring
  -> services/ingestion
  -> services/ui
```

The root `pyproject.toml` defines the shared workspace. The root `uv.lock` pins the exact package versions for that workspace. API, ingestion, UI, scoring, and dbt should normally use this lockfile.

Flink is intentionally separate:

```text
services/flink/pyproject.toml
services/flink/uv.lock
  -> services/flink/job.py
```

Flink has its own lockfile because the PyFlink Python package must match the Flink runtime image. This project uses `flink:2.2.0-scala_2.12-java17`, so the Flink Python environment pins `apache-flink==2.2.0`.

Do not move Flink back into the root uv workspace without checking dependency conflicts first. dbt and PyFlink can require incompatible versions of shared dependencies, so forcing them into one lockfile can produce a broken Flink image.

## API Test Examples

FastAPI docs:

```text
http://localhost:8000/docs
```

Example:

```text
http://localhost:8000/routes/reliable?from_lat=40.7128&from_lon=-74.0060&to_lat=40.7580&to_lon=-73.9855
```

Example with transfer:

```text
http://localhost:8000/routes/reliable?from_lat=40.7700&from_lon=-73.9180&to_lat=40.6500&to_lon=-73.9496
```

The endpoint also accepts text places:

```text
http://localhost:8000/routes/reliable?from_place=Times%20Square&to_place=Central%20Park
```

## dbt Models

The dbt project lives in:

```text
dbt/analytics
```

The model layers are:

```text
staging
  -> cleaned GTFS source tables

intermediate
  -> route frequency
  -> stop connectivity
  -> transfer risk
  -> headway stability

marts
  -> mart_route_reliability
  -> mart_route_explanations
```

Run dbt manually:

```bash
uv run --package transit-reliability-dbt dbt run --project-dir dbt/analytics --profiles-dir dbt
```

Parse/check dbt:

```bash
uv run --package transit-reliability-dbt dbt parse --project-dir dbt/analytics --profiles-dir dbt
```

## Realtime Pipeline

The realtime pipeline is:

```text
services/ingestion/realtime/gtfs_realtime_producer.py
  -> Kafka topic gtfs.trip_updates
  -> services/flink/job.py
  -> analytics.realtime_stop_reliability
  -> analytics.realtime_route_reliability
  -> services/scoring/scorer.py
```

`analytics.realtime_route_reliability` is the table consumed by the scoring layer.

`analytics.realtime_stop_reliability` is useful for analytics/debugging.

The raw high-volume trip-stop signal table was removed because it was not consumed by the application.

## Cloud Direction

The local version uses Docker Compose.

The intended cloud direction is:

* GCS bucket for raw files / data lake
* BigQuery for analytical warehouse tables
* Terraform for resource creation
* self-hosted or managed orchestration/streaming depending on cost and complexity

The Terraform folder is a scaffold, not a production-ready deployment yet.

Important: the current Terraform config requires explicit ingress CIDR ranges. Do not expose Airflow/Flink/API broadly to `0.0.0.0/0`.

## Local-Only Security Notes

The local Compose setup is intentionally developer-focused.

Known local-only compromises:

* Airflow scheduler has Docker socket access so DAGs can restart OTP locally.
* local services expose ports on localhost.
* local passwords are loaded from `.env`.
* Kafka is plaintext.

Do not copy this Compose setup directly to a public cloud VM without hardening it.

## Limitations

Known limitations:

* realtime scoring is prediction drift, not observed delay
* transfer risk is still approximate
* reliability is route-averaged, which can hide stop-level or direction-level problems
* limited automated test coverage
* cloud deployment is scaffolded but not finished
* OTP rebuild is local/Docker-specific right now

## Future Work

Planned improvements:

* validate whether MTA GTFS-Realtime feeds provide `arrival.delay` / `departure.delay`
* add actual delay modeling if the feed supports it
* improve route scoring normalization
* add automated tests for scoring and dbt assumptions
* improve observability for Flink/API/Airflow
* harden cloud deployment

## Tech Stack

* Python
* FastAPI
* Streamlit
* Postgres
* dbt
* Airflow
* Apache Kafka in KRaft mode
* Apache Flink / PyFlink
* OpenTripPlanner
* Docker Compose
* Terraform scaffold for GCP

## Status

Local deployment, static reliability modeling, realtime ingestion, route scoring, and the API/UI prototype are implemented.

The project is still a prototype. It is meant to demonstrate backend/data engineering work, not to provide production-grade transit reliability predictions yet.
