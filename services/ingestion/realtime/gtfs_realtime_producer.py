import os
import time
import requests
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

FEEDS = [
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si"
]


def create_producer():
    while True:
        try:
            print("Connecting to Kafka...", flush=True)
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: v
            )
            print("Connected to Kafka", flush=True)
            return producer
        except Exception as e:
            print(f"Kafka not ready yet: {e}", flush=True)
            time.sleep(3)


def main():
    print("Executing gtfs_realtime_producer now!", flush=True)

    producer = create_producer()

    while True:
        for feed_url in FEEDS:
            try:
                response = requests.get(feed_url, timeout=10)
                response.raise_for_status()

                payload = response.content

                producer.send(
                    "gtfs.trip_updates",
                    value=payload,
                    headers=[("feed", feed_url.encode())]
                )

                print(
                    f"sent {len(payload)} bytes from {feed_url}",
                    flush=True
                )

            except Exception as e:
                print(f"error for {feed_url}: {e}", flush=True)

        time.sleep(10)


if __name__ == "__main__":
    main()