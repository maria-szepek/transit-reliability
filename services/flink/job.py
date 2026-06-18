# Flink job for GTFS-Realtime trip updates.
#
# The job reads raw protobuf messages from Kafka, extracts predicted arrival
# times, measures how much those predictions change between feed updates, and
# writes aggregated prediction-stability metrics to Postgres.
#
# Important: this currently measures prediction drift, not schedule delay.
# A route can have high drift when realtime predictions are unstable, even if
# the vehicle is not necessarily late compared with the static GTFS schedule.

import math
import os
from contextlib import closing
from datetime import datetime, timezone

import psycopg2
from google.transit import gtfs_realtime_pb2
from pyflink.common import Time, Types
from pyflink.common.serialization import ByteArraySchema
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    ProcessWindowFunction,
    RuntimeContext,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "transit")
POSTGRES_USER = os.getenv("POSTGRES_USER", "transit")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "transit")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("GTFS_REALTIME_TOPIC", "gtfs.trip_updates")
KAFKA_GROUP_ID = os.getenv("FLINK_KAFKA_GROUP_ID", "flink-gtfs-reliability")

WINDOW_MINUTES = int(os.getenv("REALTIME_RELIABILITY_WINDOW_MINUTES", "2"))

# PyFlink tuple rows are positional. These constants make the row access below
# readable without introducing custom serialization classes.
TRIP_ID = 0
ROUTE_ID = 1
STOP_ID = 2
FEED_TIMESTAMP = 3
PREDICTION_DRIFT = 6


TRIP_UPDATE_EVENT_TYPE = Types.TUPLE([
    Types.STRING(),  # trip_id
    Types.STRING(),  # route_id
    Types.STRING(),  # stop_id
    Types.LONG(),    # arrival_time
    Types.LONG(),    # feed_timestamp
])

PREDICTION_DRIFT_SIGNAL_TYPE = Types.TUPLE([
    Types.STRING(),  # trip_id
    Types.STRING(),  # route_id
    Types.STRING(),  # stop_id
    Types.LONG(),    # feed_timestamp
    Types.LONG(),    # arrival_time
    Types.LONG(),    # previous_arrival_time
    Types.LONG(),    # prediction_drift_seconds
])

STOP_RELIABILITY_TYPE = Types.TUPLE([
    Types.STRING(),         # route_id
    Types.STRING(),         # stop_id
    Types.SQL_TIMESTAMP(),  # window_start
    Types.SQL_TIMESTAMP(),  # window_end
    Types.DOUBLE(),         # avg_abs_prediction_drift_seconds
    Types.DOUBLE(),         # stddev_prediction_drift_seconds
    Types.LONG(),           # update_count
    Types.LONG(),           # trip_count
])

ROUTE_RELIABILITY_TYPE = Types.TUPLE([
    Types.STRING(),         # route_id
    Types.SQL_TIMESTAMP(),  # window_start
    Types.SQL_TIMESTAMP(),  # window_end
    Types.DOUBLE(),         # avg_abs_prediction_drift_seconds
    Types.DOUBLE(),         # stddev_prediction_drift_seconds
    Types.LONG(),           # update_count
    Types.LONG(),           # trip_count
    Types.LONG(),           # stop_count
])


def postgres_jdbc_url() -> str:
    return f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}"


def ensure_tables() -> None:
    """Create the small serving tables used by the API and UI."""
    with closing(
        psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
    ) as conn:
        conn.autocommit = True

        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS analytics;")

            cursor.execute("""
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

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS analytics.realtime_route_reliability (
                    route_id TEXT,
                    window_start TIMESTAMP,
                    window_end TIMESTAMP,
                    avg_abs_prediction_drift_seconds DOUBLE PRECISION,
                    stddev_prediction_drift_seconds DOUBLE PRECISION,
                    update_count BIGINT,
                    trip_count BIGINT,
                    stop_count BIGINT,
                    ingestion_time TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (route_id, window_start, window_end)
                );
            """)


def decode_trip_update_arrivals(message_bytes):
    """Extract one arrival prediction row per trip/stop from a GTFS-RT message."""
    if isinstance(message_bytes, str):
        message_bytes = message_bytes.encode("latin1")

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(message_bytes)
    feed_timestamp = int(feed.header.timestamp)

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        trip = entity.trip_update.trip
        trip_id = trip.trip_id or None
        route_id = trip.route_id or None

        if not trip_id or not route_id:
            continue

        for stop_update in entity.trip_update.stop_time_update:
            # We only use arrival.time here. GTFS-RT may also contain
            # arrival.delay, but that field is optional and needs separate
            # feed validation before we base scoring on it.
            if not stop_update.stop_id or not stop_update.HasField("arrival"):
                continue

            yield (
                trip_id,
                route_id,
                stop_update.stop_id,
                int(stop_update.arrival.time),
                feed_timestamp,
            )


def window_bounds(context):
    """Convert Flink window timestamps from milliseconds to naive UTC timestamps."""
    window_start = datetime.fromtimestamp(
        context.window().start / 1000,
        tz=timezone.utc,
    ).replace(tzinfo=None)

    window_end = datetime.fromtimestamp(
        context.window().end / 1000,
        tz=timezone.utc,
    ).replace(tzinfo=None)

    return window_start, window_end


def prediction_drift_summary(rows):
    """Calculate average and spread of absolute prediction changes."""
    drifts = [abs(row[PREDICTION_DRIFT]) for row in rows]
    update_count = len(rows)

    if not update_count:
        return 0.0, 0.0, 0

    avg_abs_drift = float(sum(drifts) / update_count)

    if update_count == 1:
        return avg_abs_drift, 0.0, update_count

    variance = sum((drift - avg_abs_drift) ** 2 for drift in drifts) / update_count
    return avg_abs_drift, float(math.sqrt(variance)), update_count


class PredictionDriftFunction(KeyedProcessFunction):
    """Measures prediction changes for each trip/stop across feed updates."""

    def open(self, runtime_context: RuntimeContext):
        # State is keyed by (trip_id, stop_id). For each key we remember the
        # previous predicted arrival time and compare the next update to it.
        descriptor = ValueStateDescriptor("last_arrival_time", Types.LONG())
        self.last_arrival_time = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx):
        trip_id, route_id, stop_id, arrival_time, feed_timestamp = value
        previous_arrival_time = self.last_arrival_time.value()

        if previous_arrival_time is not None:
            yield (
                trip_id,
                route_id,
                stop_id,
                int(feed_timestamp),
                int(arrival_time),
                int(previous_arrival_time),
                int(arrival_time - previous_arrival_time),
            )

        self.last_arrival_time.update(arrival_time)


class StopReliabilityWindow(ProcessWindowFunction):
    """Aggregates prediction stability per route/stop/window."""

    def process(self, key, context, elements):
        route_id, stop_id = key
        rows = list(elements)
        avg_abs_drift, stddev_drift, update_count = prediction_drift_summary(rows)
        window_start, window_end = window_bounds(context)
        trip_count = len({row[TRIP_ID] for row in rows})

        yield (
            route_id,
            stop_id,
            window_start,
            window_end,
            avg_abs_drift,
            stddev_drift,
            int(update_count),
            int(trip_count),
        )


class RouteReliabilityWindow(ProcessWindowFunction):
    """Aggregates prediction stability per route/window for API scoring."""

    def process(self, key, context, elements):
        route_id = key
        rows = list(elements)
        avg_abs_drift, stddev_drift, update_count = prediction_drift_summary(rows)
        window_start, window_end = window_bounds(context)
        trip_count = len({row[TRIP_ID] for row in rows})
        stop_count = len({row[STOP_ID] for row in rows})

        yield (
            route_id,
            window_start,
            window_end,
            avg_abs_drift,
            stddev_drift,
            int(update_count),
            int(trip_count),
            int(stop_count),
        )


class FeedTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        # Flink event time is in milliseconds. GTFS-RT feed timestamps are in
        # Unix seconds, so multiply by 1000.
        return value[FEED_TIMESTAMP] * 1000


def create_kafka_source():
    """Read raw GTFS-Realtime protobuf payloads from Kafka."""
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(ByteArraySchema())
        .build()
    )


def create_prediction_drift_signals(env):
    """Build the stream of per-trip/per-stop prediction drift events."""
    raw_messages = env.from_source(
        create_kafka_source(),
        WatermarkStrategy.for_monotonous_timestamps(),
        "gtfs-trip-updates",
    )

    trip_update_arrivals = raw_messages.flat_map(
        decode_trip_update_arrivals,
        output_type=TRIP_UPDATE_EVENT_TYPE,
    )

    prediction_drift_signals = (
        trip_update_arrivals
        .key_by(lambda row: (row[TRIP_ID], row[STOP_ID]))
        .process(
            PredictionDriftFunction(),
            output_type=PREDICTION_DRIFT_SIGNAL_TYPE,
        )
    )

    # Windowing should use the feed creation time, not processing time, so a
    # delayed Kafka message lands in the window that matches the GTFS-RT feed.
    return prediction_drift_signals.assign_timestamps_and_watermarks(
        WatermarkStrategy
        .for_monotonous_timestamps()
        .with_timestamp_assigner(FeedTimestampAssigner())
    )


def create_stop_reliability(prediction_drift_signals):
    """Aggregate drift by route and stop for dashboard/debug analysis."""
    return (
        prediction_drift_signals
        .key_by(lambda row: (row[ROUTE_ID], row[STOP_ID]))
        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_MINUTES)))
        .process(
            StopReliabilityWindow(),
            output_type=STOP_RELIABILITY_TYPE,
        )
    )


def create_route_reliability(prediction_drift_signals):
    """Aggregate drift by route for API scoring."""
    return (
        prediction_drift_signals
        .key_by(lambda row: row[ROUTE_ID])
        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_MINUTES)))
        .process(
            RouteReliabilityWindow(),
            output_type=ROUTE_RELIABILITY_TYPE,
        )
    )


def register_postgres_sinks(t_env):
    """Register JDBC sinks that write Flink table results into Postgres."""
    jdbc_url = postgres_jdbc_url()

    t_env.execute_sql(f"""
    CREATE TABLE stop_reliability_sink (
        route_id STRING,
        stop_id STRING,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        avg_abs_prediction_drift_seconds DOUBLE,
        stddev_prediction_drift_seconds DOUBLE,
        update_count BIGINT,
        trip_count BIGINT
    ) WITH (
        'connector' = 'jdbc',
        'url' = '{jdbc_url}',
        'table-name' = 'analytics.realtime_stop_reliability',
        'username' = '{POSTGRES_USER}',
        'password' = '{POSTGRES_PASSWORD}',
        'driver' = 'org.postgresql.Driver'
    )
    """)

    t_env.execute_sql(f"""
    CREATE TABLE route_reliability_sink (
        route_id STRING,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        avg_abs_prediction_drift_seconds DOUBLE,
        stddev_prediction_drift_seconds DOUBLE,
        update_count BIGINT,
        trip_count BIGINT,
        stop_count BIGINT,
        PRIMARY KEY (route_id, window_start, window_end) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = '{jdbc_url}',
        'table-name' = 'analytics.realtime_route_reliability',
        'username' = '{POSTGRES_USER}',
        'password' = '{POSTGRES_PASSWORD}',
        'driver' = 'org.postgresql.Driver'
    )
    """)


def submit_sinks(t_env):
    """Start both streaming inserts as one Flink statement set."""
    statement_set = t_env.create_statement_set()

    statement_set.add_insert_sql("""
        INSERT INTO stop_reliability_sink
        SELECT * FROM stop_reliability
    """)

    statement_set.add_insert_sql("""
        INSERT INTO route_reliability_sink
        SELECT * FROM route_reliability
    """)

    statement_set.execute()


def main():
    ensure_tables()

    env = StreamExecutionEnvironment.get_execution_environment()
    # Keep local execution predictable. Higher parallelism needs checkpointing
    # and stronger ordering assumptions around keyed state and JDBC writes.
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    prediction_drift_signals = create_prediction_drift_signals(env)
    stop_reliability = create_stop_reliability(prediction_drift_signals)
    route_reliability = create_route_reliability(prediction_drift_signals)

    # Temporary views are the bridge from DataStream transformations to SQL
    # INSERT statements into JDBC sinks.
    t_env.create_temporary_view(
        "stop_reliability",
        t_env.from_data_stream(stop_reliability),
    )
    t_env.create_temporary_view(
        "route_reliability",
        t_env.from_data_stream(route_reliability),
    )

    register_postgres_sinks(t_env)
    submit_sinks(t_env)


if __name__ == "__main__":
    main()
