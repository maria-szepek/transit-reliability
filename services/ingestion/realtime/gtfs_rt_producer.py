import os
import time
import requests
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

FEED_URL = os.getenv(
    "GTFS_RT_URL",
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: v
)


def fetch_feed():
    response = requests.get(FEED_URL)
    response.raise_for_status()
    return response.content


def main():
    print ("Executing gtfs_rt_producer now!")
    while True:
        try:
            data = fetch_feed()

            print("payload size:", len(data))  # TODO remove later it is a debugging statement

            producer.send(
                "gtfs.trip_updates",
                value=data
            )

            print("sent GTFS realtime message")

        except Exception as e:
            print("error:", e)

        time.sleep(10)


if __name__ == "__main__":
    main()