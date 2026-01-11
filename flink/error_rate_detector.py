import json
import uuid
from datetime import datetime, timezone

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration, Time
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction


KAFKA_BROKERS = "broker2:29094"
INPUT_TOPIC = "app_logs_raw"
OUTPUT_TOPIC = "incident_alerts"

# -------------------- Timestamp Extractor --------------------
def extract_ts(event, previous_ts):
    try:
        iso_time = json.loads(event)["timestamp"]
        dt = datetime.fromisoformat(iso_time).replace(tzinfo=timezone.utc)
        ts = int(dt.timestamp() * 1000)

        print(f"[TIMESTAMP] Extracted event_time={ts} from event")

        return ts
    except Exception as e:
        print("[TIMESTAMP ERROR]", event, e)
        return 0


# -------------------- Aggregate Function --------------------
class ErrorRateAgg(AggregateFunction):

    def create_accumulator(self):
        print("[AGG] New accumulator created")
        return {"total": 0, "errors": 0}

    def add(self, value, acc):
        acc["total"] += 1
        if value["level"] == "ERROR":
            acc["errors"] += 1

        print(
            f"[AGG ADD] Service={value['service']}, "
            f"Total={acc['total']}, Errors={acc['errors']}"
        )

        return acc

    def get_result(self, acc):
        print(f"[AGG RESULT] Final accumulator: {acc}")
        return acc

    def merge(self, a, b):
        merged = {
            "total": a["total"] + b["total"],
            "errors": a["errors"] + b["errors"],
        }
        print("[AGG MERGE]", merged)
        return merged


# -------------------- Window Function --------------------
class ErrorRateWindow(ProcessWindowFunction):

    def process(self, key, context, elements):
        acc = next(iter(elements))

        total = acc["total"]
        errors = acc["errors"]
        error_rate = errors / total if total > 0 else 0

        print(
            f"\n[WINDOW FIRED]\n"
            f"  Service={key}\n"
            f"  WindowStart={context.window().start}\n"
            f"  WindowEnd={context.window().end}\n"
            f"  Total={total}, Errors={errors}, ErrorRate={error_rate:.2%}"
        )

        if total < 100:
            print("[WINDOW] Skipped → total < 100")
            return

        if error_rate >= 0.02:
            alert = {
                "incident_id": f"inc-{uuid.uuid4().hex[:8]}",
                "service": key,
                "type": "ERROR_RATE_SPIKE",
                "severity": "HIGH",
                "window_start": str(context.window().start // 1000),
                "window_end": str(context.window().end // 1000),
                "error_rate": str(round(error_rate, 4)),
                "total_logs": str(total),
            }

            print("[ALERT GENERATED]", alert)
            yield json.dumps(alert)
        else:
            print("[WINDOW] No alert → error rate normal")


# -------------------- Main --------------------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)

    env.enable_checkpointing(30000)

    print("[MAIN] Environment created")
    print("[MAIN] Parallelism = 3, Checkpointing = 30s")

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics(INPUT_TOPIC)
        .set_group_id("error-rate-detector")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    print(f"[MAIN] Kafka source configured for topic '{INPUT_TOPIC}'")

 
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(extract_ts)
        .with_idleness(Duration.of_seconds(10))
    )

    print("[MAIN] Watermark strategy configured")

    stream = (
        env.from_source(source, watermark_strategy, "kafka-source")
        .map(
            lambda x: (
                print("[SOURCE] Consumed from Kafka:", x) or json.loads(x)
            ),
            Types.MAP(Types.STRING(), Types.STRING()),
        )
        .map(
            lambda x: (
                print("[PARSED STREAM]", x) or x
            )
        )
    )

    print("[MAIN] Stream parsing added")

    alerts = (
        stream
        .key_by(lambda x: x["service"], Types.STRING())
        .window(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(10)))
        .aggregate(ErrorRateAgg(), ErrorRateWindow())
        .map(
            lambda x: (
                print("[SINK INPUT] Sending to Kafka:", x) or x
            ),
            Types.STRING(),
        )
    )

    print("[MAIN] Window + aggregation configured")

    record_serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(OUTPUT_TOPIC)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_record_serializer(record_serializer)
        .build()
    )

    alerts.sink_to(sink)

    print(f"[MAIN] Kafka sink configured for topic '{OUTPUT_TOPIC}'")
    print("[MAIN] Submitting job to Flink")

    env.execute("error-rate-spike-detector")


if __name__ == "__main__":
    main()
