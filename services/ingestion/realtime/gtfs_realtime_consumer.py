import os
import time
import psycopg2
from kafka import KafkaConsumer
from google.transit import gtfs_realtime_pb2

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "gtfs.trip_updates"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "transit")
POSTGRES_USER = os.getenv("POSTGRES_USER", "transit")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "transit")


def wait_for_postgres():
    while True:
        try:
            print("Connecting to Postgres...", flush=True)
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            print("Connected to Postgres", flush=True)
            return conn
        except Exception as e:
            print(f"Postgres not ready: {e}", flush=True)
            time.sleep(3)


def wait_for_kafka():
    while True:
        try:
            print("Connecting to Kafka...", flush=True)
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                # auto_offset_reset="latest",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                #     group_id="gtfs-consumer" 
            )
            print("Connected to Kafka", flush=True)
            return consumer
        except Exception as e:
            print(f"Kafka not ready: {e}", flush=True)
            time.sleep(3)


def ensure_schema(cursor):
    cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS raw_realtime;
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_realtime.stop_time_updates (
            trip_id TEXT,
            route_id TEXT,
            stop_id TEXT,
            arrival_time BIGINT,
            departure_time BIGINT,
            feed_timestamp BIGINT,
            ingestion_time TIMESTAMP DEFAULT NOW()
        );
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_rt_trip
        ON raw_realtime.stop_time_updates (trip_id);
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_rt_stop
        ON raw_realtime.stop_time_updates (stop_id);
    """)

    cursor.execute("""        
        CREATE INDEX IF NOT EXISTS idx_rt_time
        ON raw_realtime.stop_time_updates (feed_timestamp);
    """)


def main():
    conn = wait_for_postgres()
    cursor = conn.cursor()

    ensure_schema(cursor)
    conn.commit()

    consumer = wait_for_kafka()

    print("Consumer started", flush=True)

    for message in consumer:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(message.value)

        # TODO remove debugging statement
        # from google.protobuf.json_format import MessageToDict
        # print("MessageToDict(feed) is: ")
        # print(MessageToDict(feed))

        inserted = 0

        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue

            trip = entity.trip_update.trip

            trip_id = trip.trip_id or None
            route_id = trip.route_id or None
            start_date = trip.start_date or None
            feed_ts = feed.header.timestamp

            for stop_update in entity.trip_update.stop_time_update:
                stop_id = stop_update.stop_id or None

                arrival = (
                    stop_update.arrival.time
                    if stop_update.HasField("arrival")
                    else None
                )

                departure = (
                    stop_update.departure.time
                    if stop_update.HasField("departure")
                    else None
                )

                cursor.execute(
                    """
                    INSERT INTO raw_realtime.stop_time_updates (
                        trip_id,
                        route_id,
                        stop_id,
                        arrival_time,
                        departure_time,
                        feed_timestamp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        trip_id,
                        route_id,
                        stop_id,
                        arrival,
                        departure,
                        feed_ts
                    )
                )

                inserted += 1

        conn.commit()
        print(f"Inserted {inserted} records", flush=True)


if __name__ == "__main__":
    main()