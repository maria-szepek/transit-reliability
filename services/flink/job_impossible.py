import math
from datetime import datetime, timezone

import psycopg2
from google.transit import gtfs_realtime_pb2

from pyflink.common import Types, Time
# from pyflink.common.serialization import ByteArrayDeserializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import KeyedProcessFunction, ProcessWindowFunction, RuntimeContext
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.connectors.jdbc import (
    JdbcConnectionOptions,
    JdbcExecutionOptions,
    JdbcSink,
)


POSTGRES_HOST = "postgres"
POSTGRES_DB = "transit"
POSTGRES_USER = "transit"
POSTGRES_PASSWORD = "transit"

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "gtfs.trip_updates"


def ensure_tables() -> None:
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS analytics.realtime_trip_stop_signals (
            trip_id TEXT,
            route_id TEXT,
            stop_id TEXT,
            feed_timestamp BIGINT,
            arrival_time BIGINT,
            previous_arrival_time BIGINT,
            prediction_drift_seconds BIGINT,
            ingestion_time TIMESTAMP DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_rt_trip_stop_signals_trip_stop_ts
        ON analytics.realtime_trip_stop_signals (trip_id, stop_id, feed_timestamp);
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_rt_trip_stop_signals_route_stop_ts
        ON analytics.realtime_trip_stop_signals (route_id, stop_id, feed_timestamp);
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS analytics.realtime_stop_reliability (
            route_id TEXT,
            stop_id TEXT,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            avg_abs_prediction_drift_seconds DOUBLE PRECISION,
            stddev_prediction_drift_seconds DOUBLE PRECISION,
            update_count BIGINT,
            trip_count BIGINT,
            ingestion_time TIMESTAMP DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_rt_stop_reliability_route_stop_window
        ON analytics.realtime_stop_reliability (route_id, stop_id, window_start);
    """)

    cur.close()
    conn.close()


def decode_gtfs(message_bytes):
    if isinstance(message_bytes, str):
        message_bytes = message_bytes.encode("latin1")

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(message_bytes)

    feed_ts = int(feed.header.timestamp)

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        trip = entity.trip_update.trip
        trip_id = trip.trip_id or None
        route_id = trip.route_id or None

        if not trip_id or not route_id:
            continue

        for stop_update in entity.trip_update.stop_time_update:
            if not stop_update.stop_id:
                continue

            if not stop_update.HasField("arrival"):
                continue

            arrival_time = int(stop_update.arrival.time)
            stop_id = stop_update.stop_id

            yield (
                trip_id,
                route_id,
                stop_id,
                arrival_time,
                feed_ts,
            )


class DriftFunction(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        self.last_arrival = runtime_context.get_state(
            "last_arrival",
            Types.LONG()
        )

    def process_element(self, value, ctx):
        trip_id, route_id, stop_id, arrival_time, feed_timestamp = value

        previous_arrival = self.last_arrival.value()

        if previous_arrival is not None:
            prediction_drift = int(arrival_time - previous_arrival)

            yield (
                trip_id,
                route_id,
                stop_id,
                int(feed_timestamp),
                int(arrival_time),
                int(previous_arrival),
                int(prediction_drift),
            )

        self.last_arrival.update(arrival_time)


class StopReliabilityWindow(ProcessWindowFunction):

    def process(self, key, context, elements):
        route_id, stop_id = key
        rows = list(elements)

        drifts = [abs(r[6]) for r in rows]
        update_count = len(rows)
        trip_count = len({r[0] for r in rows})

        avg_abs_drift = float(sum(drifts) / update_count) if update_count else 0.0

        if update_count > 1:
            mean = avg_abs_drift
            variance = sum((d - mean) ** 2 for d in drifts) / update_count
            stddev = float(math.sqrt(variance))
        else:
            stddev = 0.0

        window_start = datetime.fromtimestamp(
            context.window().start / 1000,
            tz=timezone.utc,
        ).replace(tzinfo=None)

        window_end = datetime.fromtimestamp(
            context.window().end / 1000,
            tz=timezone.utc,
        ).replace(tzinfo=None)

        yield (
            route_id,
            stop_id,
            window_start,
            window_end,
            avg_abs_drift,
            stddev,
            int(update_count),
            int(trip_count),
        )


def build_trip_stop_sink():
    return JdbcSink.sink(
        """
        INSERT INTO analytics.realtime_trip_stop_signals (
            trip_id,
            route_id,
            stop_id,
            feed_timestamp,
            arrival_time,
            previous_arrival_time,
            prediction_drift_seconds
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        Types.TUPLE([
            Types.STRING(),  # trip_id
            Types.STRING(),  # route_id
            Types.STRING(),  # stop_id
            Types.LONG(),    # feed_timestamp
            Types.LONG(),    # arrival_time
            Types.LONG(),    # previous_arrival_time
            Types.LONG(),    # prediction_drift_seconds
        ]),
        JdbcExecutionOptions.builder()
            .with_batch_size(200)
            .with_batch_interval_ms(500)
            .build(),
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url(f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}")
            .with_driver_name("org.postgresql.Driver")
            .with_user_name(POSTGRES_USER)
            .with_password(POSTGRES_PASSWORD)
            .build()
    )


def build_stop_reliability_sink():
    return JdbcSink.sink(
        """
        INSERT INTO analytics.realtime_stop_reliability (
            route_id,
            stop_id,
            window_start,
            window_end,
            avg_abs_prediction_drift_seconds,
            stddev_prediction_drift_seconds,
            update_count,
            trip_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        Types.TUPLE([
            Types.STRING(),    # route_id
            Types.STRING(),    # stop_id
            Types.SQL_TIMESTAMP(),  # window_start
            Types.SQL_TIMESTAMP(),  # window_end
            Types.DOUBLE(),    # avg_abs_prediction_drift_seconds
            Types.DOUBLE(),    # stddev_prediction_drift_seconds
            Types.LONG(),      # update_count
            Types.LONG(),      # trip_count
        ]),
        JdbcExecutionOptions.builder()
            .with_batch_size(100)
            .with_batch_interval_ms(1000)
            .build(),
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url(f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}")
            .with_driver_name("org.postgresql.Driver")
            .with_user_name(POSTGRES_USER)
            .with_password(POSTGRES_PASSWORD)
            .build()
    )


def main():
    ensure_tables()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # source = KafkaSource.builder() \
    #     .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
    #     .set_topics(TOPIC) \
    #     .set_group_id("flink-gtfs-reliability") \
    #     .set_value_only_deserializer(ByteArrayDeserializer()) \
    #     .build()

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_topics(TOPIC) \
        .set_group_id("flink-gtfs-reliability") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        source,
        WatermarkStrategy.for_monotonous_timestamps(),
        "gtfs-trip-updates",
    )

    flat_events = stream.flat_map(
        decode_gtfs,
        output_type=Types.TUPLE([
            Types.STRING(),  # trip_id
            Types.STRING(),  # route_id
            Types.STRING(),  # stop_id
            Types.LONG(),    # arrival_time
            Types.LONG(),    # feed_timestamp
        ])
    )

    trip_stop_signals = (
        flat_events
        .key_by(lambda x: (x[0], x[2]))
        .process(
            DriftFunction(),
            output_type=Types.TUPLE([
                Types.STRING(),  # trip_id
                Types.STRING(),  # route_id
                Types.STRING(),  # stop_id
                Types.LONG(),    # feed_timestamp
                Types.LONG(),    # arrival_time
                Types.LONG(),    # previous_arrival_time
                Types.LONG(),    # prediction_drift_seconds
            ])
        )
    )

    trip_stop_signals.add_sink(build_trip_stop_sink())

    stop_reliability = (
        trip_stop_signals
        .key_by(lambda x: (x[1], x[2]))  # route_id, stop_id
        .window(TumblingEventTimeWindows.of(Time.minutes(2)))
        .process(
            StopReliabilityWindow(),
            output_type=Types.TUPLE([
                Types.STRING(),         # route_id
                Types.STRING(),         # stop_id
                Types.SQL_TIMESTAMP(),  # window_start
                Types.SQL_TIMESTAMP(),  # window_end
                Types.DOUBLE(),         # avg_abs_prediction_drift_seconds
                Types.DOUBLE(),         # stddev_prediction_drift_seconds
                Types.LONG(),           # update_count
                Types.LONG(),           # trip_count
            ])
        )
    )

    stop_reliability.add_sink(build_stop_reliability_sink())

    env.execute("gtfs-realtime-reliability")


if __name__ == "__main__":
    main()