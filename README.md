# (What I want to do next) : 

* update README: good 1. context, 2. technical documentation + reasoning, 3. instructions to deploy + test, 4. add screenshots of the application usage and testing, also for the other services, and add screenshots for the data lineage graphs (dbt for example) 5. clear limitation documentation, 6. clear + actionable future work documentation   
* Good user friendly error logging on ui when routes can not be retrieved yet, because OTP not done building the graph 
* review project
* rework Makefile
* Add tests
* CI/CD pipeline


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
* optional GCS + BigQuery backend for static GTFS analytics

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
  -> loads GTFS into Postgres or BigQuery, depending on WAREHOUSE_BACKEND
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
  -> raw GTFS tables and dbt analytics models in postgres mode
  -> realtime reliability tables

GCS + BigQuery
  -> raw GTFS zip archive in GCS
  -> raw GTFS tables and dbt analytics models in bigquery mode

OpenTripPlanner
  -> builds routing graph from GTFS + OSM
  -> returns candidate itineraries

FastAPI
  -> receives route requests
  -> asks OpenTripPlanner for itineraries
  -> scores routes using static warehouse scores + local realtime reliability signals

Streamlit
  -> simple route search and analytics UI
```

## Main Services

Postgres: local warehouse for raw GTFS, dbt models, and realtime reliability tables in postgres mode. Realtime serving tables still live here in bigquery mode.

GCS: raw GTFS zip archive storage in bigquery mode.

BigQuery: raw GTFS tables and dbt analytics models in bigquery mode.

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

Copy the example and edit only what you need:

```bash
cp .env.example .env
```

Minimal local example:

```env
WAREHOUSE_BACKEND=postgres
GCP_PROJECT_ID=

POSTGRES_HOST=postgres
POSTGRES_USER=transit
POSTGRES_PASSWORD=transit
POSTGRES_DB=transit
POSTGRES_PORT=5432

PGADMIN_DEFAULT_EMAIL=admin@admin.com
PGADMIN_DEFAULT_PASSWORD=admin
```

For the BigQuery backend:

```env
WAREHOUSE_BACKEND=bigquery
GCP_PROJECT_ID=gcp-project-id

POSTGRES_HOST=postgres
POSTGRES_USER=transit
POSTGRES_PASSWORD=transit
POSTGRES_DB=transit
POSTGRES_PORT=5432
```

The Airflow local defaults live in `docker-compose.yml` so the normal user-facing `.env` stays small. For anything public/cloud-facing, do not use default passwords.

### 3. Start the stack

```bash
docker compose up -d --build
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

The refresh DAGs download data, load GTFS into the selected backend, run dbt, and rebuild OTP locally.

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


![alt text](<Screenshot from 2026-06-18 09-38-21.png>)



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

The dbt target is selected by `WAREHOUSE_BACKEND`:

```text
WAREHOUSE_BACKEND=postgres  -> Postgres target
WAREHOUSE_BACKEND=bigquery  -> BigQuery target
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

In the BigQuery backend, realtime aggregates still remain in local Postgres. Direct realtime output to BigQuery is a later extension. Doing that cleanly would require adding a cloud-native sink such as Pub/Sub, GCS load files, or a dedicated BigQuery writer.

## BigQuery Backend

The default backend is local Postgres. The optional backend is hybrid:

```text
local Docker Compose runtime
GCS for raw GTFS zip archives
BigQuery for raw GTFS tables and dbt analytics models
local Postgres for realtime serving tables
```

Create GCP resources with Terraform:

```bash
make infra-plan
make infra-apply
```

Terraform creates:

* one GCS bucket for raw GTFS zip files
* BigQuery datasets: `raw`, `analytics`, `realtime`
* a service account and IAM permissions for GCS/BigQuery access

The bucket name is derived from the GCP project:

```text
<GCP_PROJECT_ID>-transit-reliability-raw
```

The BigQuery path uses partitioning and clustering where it matches the actual query pattern:

* `stg_stop_times`: partitioned by `arrival_hour`, clustered by `trip_id` and `stop_id`
* `int_transfer_events`: partitioned by `arrival_seconds`, clustered by `station_id`, `route_id`, and `departure_seconds`
* `int_transfer_risk`: clustered by `route_id`, `station_id`, and `to_route_id`
* `mart_route_reliability`: clustered by `route_id`

The Postgres backend uses indexes for the same purpose. Real Postgres table partitioning is intentionally not used because it would add maintenance complexity without much benefit for this local-sized warehouse.

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
* BigQuery backend is implemented as a hybrid local/cloud path, not a full cloud deployment
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
* Terraform for GCP resources
* Google Cloud Storage
* BigQuery

## Status

Local deployment, static reliability modeling, realtime ingestion, route scoring, the API/UI prototype, and the hybrid BigQuery backend are implemented.

The project is still a prototype. It is meant to demonstrate backend/data engineering work, not to provide production-grade transit reliability predictions yet.



