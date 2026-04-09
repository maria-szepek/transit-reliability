<!-- GTFS realtime feed
        ↓
Kafka (optional later)
        ↓
Ingestion service
        ↓
Postgres raw_realtime schema
        ↓
dbt models
        ↓
dynamic scoring -->

GTFS Realtime API
        ↓
Kafka Producer (Python)
        ↓
Kafka Topic (trip_updates)
        ↓
Kafka Consumer (Python)
        ↓
Postgres raw_realtime
        ↓
dbt models
        ↓
API scoring

### GTFS-Realtime actually has three feed types:

* Trip Updates – delays for scheduled trips
** Contains delay vs schedule; Directly usable for:
*** on-time probability
*** delay variance
*** transfer failure probability


* Vehicle Positions – real-time GPS location of vehicles
* Service Alerts – disruptions, closures, etc.