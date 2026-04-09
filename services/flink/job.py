import math
from datetime import datetime, timezone

import psycopg2
from google.transit import gtfs_realtime_pb2

from pyflink.common import Types, Time
from pyflink.common.serialization import ByteArraySchema
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    # KafkaRecordDeserializationSchema,
)
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    ProcessWindowFunction,
    RuntimeContext,
)

from pyflink.datastream.state import ValueStateDescriptor 
from pyflink.datastream.window import TumblingEventTimeWindows
# from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.common.watermark_strategy import TimestampAssigner


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

    cur.execute("CREATE SCHEMA IF NOT EXISTS analytics;")

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

    from pyflink.datastream.state import ValueStateDescriptor

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("last_arrival", Types.LONG())
        self.last_arrival = runtime_context.get_state(descriptor)

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

class FeedTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value, record_timestamp):
        return value[3] * 1000   # feed_timestamp → milliseconds


def main():
    ensure_tables()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_topics(TOPIC) \
        .set_group_id("flink-gtfs-reliability") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(ByteArraySchema()) \
        .build()

    stream = env.from_source(
        source,
        WatermarkStrategy.for_monotonous_timestamps(),
        "gtfs-trip-updates",
    )

    flat_events = stream.flat_map(
        decode_gtfs,
        output_type=Types.TUPLE([
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.LONG(),
            Types.LONG(),
        ])
    )

    trip_stop_signals = (
        flat_events
        .key_by(lambda x: (x[0], x[2]))
        .process(
            DriftFunction(),
            output_type=Types.TUPLE([
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.LONG(),
                Types.LONG(),
                Types.LONG(),
                Types.LONG(),
            ])
        )
    )

    # trip_stop_signals = trip_stop_signals.assign_timestamps_and_watermarks(
    #     WatermarkStrategy
    #         .for_monotonous_timestamps()
    #         .with_timestamp_assigner(
    #             lambda event, ts: event[3] * 1000   # feed_timestamp → milliseconds
    #         )
    # )

    trip_stop_signals = trip_stop_signals.assign_timestamps_and_watermarks(
        WatermarkStrategy
            .for_monotonous_timestamps()
            .with_timestamp_assigner(FeedTimestampAssigner())
    )

    stop_reliability = (
        trip_stop_signals
        .key_by(lambda x: (x[1], x[2]))
        .window(TumblingEventTimeWindows.of(Time.minutes(2)))
        .process(
            StopReliabilityWindow(),
            output_type=Types.TUPLE([
                Types.STRING(),
                Types.STRING(),
                Types.SQL_TIMESTAMP(),
                Types.SQL_TIMESTAMP(),
                Types.DOUBLE(),
                Types.DOUBLE(),
                Types.LONG(),
                Types.LONG(),
            ])
        )
    )

    trip_table = t_env.from_data_stream(trip_stop_signals)
    stop_table = t_env.from_data_stream(stop_reliability)

    t_env.create_temporary_view("trip_stop_signals", trip_table)
    t_env.create_temporary_view("stop_reliability", stop_table)

    t_env.execute_sql("""
    CREATE TABLE trip_stop_sink (
        trip_id STRING,
        route_id STRING,
        stop_id STRING,
        feed_timestamp BIGINT,
        arrival_time BIGINT,
        previous_arrival_time BIGINT,
        prediction_drift_seconds BIGINT
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/transit',
        'table-name' = 'analytics.realtime_trip_stop_signals',
        'username' = 'transit',
        'password' = 'transit',
        'driver' = 'org.postgresql.Driver'
    )
    """)

    t_env.execute_sql("""
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
        'url' = 'jdbc:postgresql://postgres:5432/transit',
        'table-name' = 'analytics.realtime_stop_reliability',
        'username' = 'transit',
        'password' = 'transit',
        'driver' = 'org.postgresql.Driver'
    )
    """)

    statement_set = t_env.create_statement_set()

    statement_set.add_insert_sql("""
        INSERT INTO trip_stop_sink
        SELECT * FROM trip_stop_signals
    """)

    statement_set.add_insert_sql("""
        INSERT INTO stop_reliability_sink
        SELECT * FROM stop_reliability
    """)

    statement_set.execute()


if __name__ == "__main__":
    main()