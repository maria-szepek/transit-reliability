# transit-reliability
DTCDE final project

# what i did 

* created repo structure 
mkdir -p {docker/{otp,postgres,dagster,kafka},services/{api,ingestion,scoring},dbt,dagster_project,data/{gtfs,osm}}
touch docker-compose.yml README.md

* wrote docker-compose.yml
* created .env file

* wrote airflow/docker-compose.yml and created .env

docker compose down -v
docker compose up -d

* tested services: 
** airflow -> localhost:8080 
** postgres (transit) and postgres (airflow) ->  docker exec -ti airflow-postgres-1 psql -U transit -d transit
** kafka -> docker exec -it transit-reliability-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list / docker exec -it transit-reliability-kafka-1 \
kafka-topics --bootstrap-server localhost:9092 \
--create \
--topic test-topic

# Add OTP 
** https://docs.opentripplanner.org/en/latest/Basic-Tutorial/


* docker compose file: ????? whatever ahppenend there 
* dwl wget https://download.geofabrik.de/north-america/us/new-york-latest.osm.pbf to data/osm https://download.geofabrik.de/north-america/us/new-york.html
* dwl wget https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip to data/gtfs
### actually i found this developer resources provided by mta: https://www.mta.info/developers
* https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip
* https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip
* https://rrgtfsfeeds.s3.amazonaws.com/gtfslirr.zip
* https://rrgtfsfeeds.s3.amazonaws.com/gtfsmnr.zip
* https://www.njtransit.com/developer-tools

You need:

MTA subway
MTA bus
LIRR
Metro-North
NJ Transit rail
NJ Transit bus
PATH

That gives full NYC metro coverage.

after checking transitland as well which looked tedious, i found: 

# Found ideal source!!! 
https://data.ny.gov/Transportation/MTA-General-Transit-Feed-Specification-GTFS-Static/fgm6-ccue/about_data


* test: 1003 Morris Avenue, Union, New Jersey 07083, USA to 15 River St, Brooklyn, NY 11249, USA

* brought up otp and test on: http://localhost:8088/otp/routers/default
http://localhost:8088/otp/routers/default/plan?fromPlace=40.7505,-73.9934&toPlace=40.7128,-74.0060&time=08:00am&date=03-25-2026&mode=TRANSIT,WALK

# phase 2 ingestion 

## I then created ingestion python folder
* init uv 
* installed dependencies 
* create docker file 
* created ingestion script load_gtfs.py
* build container 

run : 

docker compose exec postgres psql -U transit -d transit -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"

docker compose run  --remove-orphans ingestion 2>&1 | tee ingestion.log


## how i use duckdb to view the data: 
duckdb -ui 
INSTALL postgres;
LOAD postgres;

ATTACH 'host=localhost user=transit password=transit dbname=transit port=5432'
AS transit (TYPE POSTGRES);

-- and how i use pgcli: 
uv run 

# phase 3 

1. route_frequency        (service density)
2. stop_connectivity      (reroute resilience)
3. transfer_windows       (transfer risk)
4. trip_performance       (delay / variance)

-> 

Feature tables              → Reliability metric
------------------------------------------------
trip_performance            → on-time probability
trip_performance            → delay variance
transfer_windows            → transfer failure probability
route_frequency             → time-of-day reliability
stop_connectivity           → reroute resilience
route_frequency + connectivity → reroute resilience


Core features: 
Reliability scoring
Transfer risk
Delay variance
Reroute resilience
Ranking
Explainability



* dbt runs: uv run dbt run --full-refresh / dbt run --select staging marts


### api ...
* test locally: uv run --project services/api \
  uvicorn services.api.main:app --reload --port 8000

* from: 40.7128,-74.0060   → Lower Manhattan (City Hall / Wall St)
* to:   40.7580,-73.9855   → Times Square
* -> http://localhost:8000/routes/reliable?from_lat=40.7128&from_lon=-74.0060&to_lat=40.7580&to_lon=-73.9855

* containarized with dockerfile and docker compose up --build api , then http://localhost:8000/docs

* amazing test: 
Test 1 — Astoria → Brooklyn (almost always 1 transfer)
http://localhost:8000/routes/reliable?from_lat=40.7700&from_lon=-73.9180&to_lat=40.6500&to_lon=-73.9496


# questions 
* what is zookeeper 
* why are we giving container_names: it will be confusing no?

* should we consider to use redpanda instead of kafka? 
* should we consider to switch to Kafka KRaft (no ZooKeeper)

* restart: unless-stopped / restart: on-failure.. what are the best policies for all the containers? 

* should i consider using dagster/airflow/kestra for the project? -> for now i tried with dagster it was annoying so ill go with kestra.
* now im using airflow: https://www.youtube.com/watch?v=PbSIVDou17Q /
https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html / 
https://airflow.apache.org/docs/docker-stack/build.html

* currently api is returning duplicated routes( same route + same scoring but for some reason duplicates on different ranks)



# important question about the core evaluation: 
is route resilience with avg avg_stop_connectivity a robust way of measuring that ?? 
apparently my data has now hours per day 0, 1, 2, 3, ... 24 (apparently 1 hour too much ) see analytics.int_route_frequency
mart_route_reliability uses: -- normalized score (simple first version) -- that can be revisited
* score at (agency_id, route_id) if business understanding would justify it ? i have to understand first if that would make a difference in reality, or not :/ 
* the transfer LEGS can also be a source for confusion and unreliability, later that should be improved somehow

# usage scenario
* user does NOT usually look up latitude + longitude coordinates, it makes no sense. we need geocoding: address-lat/lon-OTP-routes-scoring: 
OpenStreetMap Nominatim (free), or Mapbox / Google later?

* add "fastest": true vs "most_reliable": true, or just show durations ... 


# TESTING 
* i havent written any tests yet.


# IMPORTANT PERF IMPROVEMENTS 
```
create index if not exists idx_stop_times_stop
on raw.stop_times(stop_id);

create index if not exists idx_stop_times_trip
on raw.stop_times(trip_id);

create index if not exists idx_stop_times_arrival
on raw.stop_times(arrival_time);

create index if not exists idx_trips_trip
on raw.trips(trip_id);

create index if not exists idx_transfers_stop
on raw.transfers(from_stop_id, to_stop_id);
```