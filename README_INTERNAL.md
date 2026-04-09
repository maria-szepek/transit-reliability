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


(or , but i didnt try that: docker compose run --rm \
  -v ./data:/data \
  ingestion \
  python -m gtfs.load_gtfs)

  or : docker compose --profile batch run --rm static-gtfs


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


# realtime data ingestion
* used: 

Realtime data - Subway, rail, and alerts

Subway Realtime Feeds: 
https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace
https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm
https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g
https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz
https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw
https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l
https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs
https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si

LIRR Realtime Feeds.
 https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/lirr%2Fgtfs-lirr
 
 MNR Realtime Feeds.
 https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/mnr%2Fgtfs-mnr
 
 Service Alert Feeds.
 https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fall-alerts
 https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts
 https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fbus-alerts
 https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Flirr-alerts
 https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fmnr-alerts
 
 respectively 
 https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fall-alerts.json
 https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts.json
 https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fbus-alerts.json
 https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Flirr-alerts.json
 https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fmnr-alerts.json
 
 


Realtime data - Buses

API KEY REQUESTED - https://bt.mta.info/wiki/Developers/Index -> api key=1be029c3-7d96-4b53-bf70-5a19a69a9ebc
Documentation: 


* tested topic creation: docker exec -it <kafka_container> kafka-topics --bootstrap-server localhost:9092 --list



### how i tested the flink job i createe: 
```
docker exec -it transit-reliability-flink-jobmanager-1 \
flink run -py /opt/flink/usrlib/job.py
```
and how i rebuild it :
```
docker compose stop flink-jobmanager flink-taskmanager
docker compose rm -f flink-jobmanager flink-taskmanager
docker compose build --no-cache flink-jobmanager flink-taskmanager
docker compose up -d flink-jobmanager flink-taskmanager
```


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

* is there any use of elevator / escalator alerts feed? for now i dont see any reason to use that.

* for now we are just using static gtfs data and static osm file, question is how regularly we should update this. 



# important question about the core evaluation: 
is route resilience with avg avg_stop_connectivity a robust way of measuring that ?? 
apparently my data has now hours per day 0, 1, 2, 3, ... 24 (apparently 1 hour too much ) see analytics.int_route_frequency
mart_route_reliability uses: -- normalized score (simple first version) -- that can be revisited
* score at (agency_id, route_id) if business understanding would justify it ? i have to understand first if that would make a difference in reality, or not :/ 
* the transfer LEGS can also be a source for confusion and unreliability, later that should be improved somehow

* what is a schema registry and do i need that in my project,  is it applicable at all. no schema registry (yet)

* if the consumer keeps pushing data into the realtime data table, is there ever going to be a cleanup or what ??? IMPORTANT

### GTFS-Realtime actually has three feed types:

* Trip Updates – delays for scheduled trips
* Vehicle Positions – real-time GPS location of vehicles
* Service Alerts – disruptions, closures, etc.

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


# IMPORTANT TODOS
* load_gtfs.py appends, find good rule to avoid duplications 
* 



# informations for myself: 
* gtfs-realtime-bindings: gtfs-realtime-bindings are official, language-specific libraries (generated from Protocol Buffer specifications) used to parse binary GTFS-realtime transit data into usable objects. They are required for developers building apps that consume live transit feeds (vehicle locations, alerts, delays) to interpret the data format, maintained by MobilityData. 

# data source for realime data
* https://www.mta.info/developers
*  


* SUPER IMPORTANT CONCEPTS 
The Core GTFS Concepts

Think of GTFS like this hierarchy:

Route → Trip → Stop Times → Stops

A route is the service line passengers recognize: e.g. Subway: A train, Bus: M15, Express: Q44 SBS

A trip is one vehicle traveling along a route at a specific time: 

Example (Route = A train):

Trip 1 → leaves at 08:00
Trip 2 → leaves at 08:08
Trip 3 → leaves at 08:16

Each of those is a different trip.

-> Route = "A train", Trip = "A train at 08:00 AM".

# improvements for realtime data consumption: 

. Example from your data

You may observe:

12:00  Train 147150  Stop 234N → arrives 12:05
12:01  Train 147150  Stop 234N → arrives 12:07
12:02  Train 147150  Stop 234N → arrives 12:06

This means:

prediction changed
uncertainty exists
reliability is lower
2. First metric — prediction drift

We compute:

drift = arrival_time(t) - arrival_time(t-1)

Example:

12:05 → 12:07  = +2 min delay
12:07 → 12:06  = -1 min correction

Large drift = unreliable service

3. Second metric — variance of predictions

For each (trip_id, stop_id):

variance(arrival_time)

Low variance → reliable
High variance → unreliable

4. Third metric — delay growth

If predictions keep increasing:

12:05 → 12:07 → 12:09 → 12:12

This is delay propagation.
This tells you:

congestion
degraded line
low reliability
5. Fourth metric — headway stability

Example:

Train A arrives 12:05
Train B arrives 12:07

Headway = 2 minutes

If next update:

Train A arrives 12:08
Train B arrives 12:09

Headway collapses → train bunching

This is major reliability issue.

6. Fifth metric — transfer risk

Example:

Line A arrives 12:05
Line B departs 12:06

Margin = 1 minute

If prediction shifts:

arrival moves to 12:07

Transfer becomes impossible.
Reliability drops.


### maybe 
* rename data to data_lake

### very important, do not forget!!! 
* need make cloud-destroy
* 