import json
import uuid
from datetime import datetime, timezone

from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction
from pyflink.datastream.functions import RuntimeContext
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Time, Duration
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction

# ---------------- CONFIG ----------------
P95_THRESHOLD_MS = 100
BREACH_LIMIT = 3

KAFKA_BROKERS = "broker2:29094"
INPUT_TOPIC = "app_logs_raw"
OUTPUT_TOPIC = "incident_alerts"


# ---------------- TIMESTAMP ----------------
def extract_ts(e, ts):
    try:
        iso_time = json.loads(e)["timestamp"]
        dt = datetime.fromisoformat(iso_time).replace(tzinfo=timezone.utc)
        event_ts = int(dt.timestamp() * 1000)

        print(f"[TIMESTAMP] Extracted event_time={event_ts} from record")

        return event_ts
    except Exception as ex:
        print("[TIMESTAMP ERROR]", e, ex)
        return 0


class P95Window(ProcessWindowFunction):
    def process(self, key, context, elements):
        latencies = [int(e["latency_ms"]) for e in elements]
        if not latencies:
            return

        latencies.sort()
        idx = int(0.95 * (len(latencies) - 1))
        p95 = latencies[idx]

        alert_dict = {
            "service": key,
            "p95": p95,  # keep int for logic
            "window_start": context.window().start // 1000,
            "window_end": context.window().end // 1000,
        }

        print("[WINDOW FIRED]", alert_dict)
        # emit JSON string
        yield alert_dict


# ---------------- BREACH TRACKER ----------------
class BreachDetector(KeyedProcessFunction):
    def open(self, ctx: RuntimeContext):
        print("[OPEN] Initializing keyed state for breach_count")
        self.breach_count = ctx.get_state(
            ValueStateDescriptor("breach_count", Types.INT())
        )

    def process_element(self, value, ctx):
        service = value["service"]
        p95 = value["p95"]
        current_count = self.breach_count.value() or 0

        print(
            f"[PROCESS] Service={service}, P95={p95}, Previous breaches={current_count}"
        )

        if p95 > P95_THRESHOLD_MS:
            current_count += 1
            print("[PROCESS] p95 breached threshold", current_count)
        else:
            current_count = 0
            print("[PROCESS] p95 normal → reset breach counter", current_count)

        self.breach_count.update(current_count)

        if current_count >= BREACH_LIMIT:
            alert = {
                "incident_id": f"inc-{uuid.uuid4().hex[:8]}",
                "service": service,
                "type": "LATENCY_SLO_BREACH",
                "severity": "CRITICAL",
                "p95_latency": p95,
                "breach_count": current_count,
                "window_start":value["window_start"],
                "window_end":value["window_end"],
            }
            print("[ALERT GENERATED]", alert)
            self.breach_count.clear()
            print("[STATE CLEAR] breach_count reset after alert")

            # emit alert as dict
            yield alert


# ---------------- MAIN ----------------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(3)

    print("[JOB] Starting Latency SLO Monitor Job")

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics(INPUT_TOPIC)
        .set_group_id("latency-slo-monitor")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_idleness(Duration.of_seconds(10))
        .with_timestamp_assigner(extract_ts)
    )

    stream = env.from_source(source, watermark, "kafka-source").map(
        lambda x: (print("[SOURCE] Consumed from Kafka:", x) or json.loads(x))
    )

    alerts = (
        stream.key_by(lambda x: x["service"])
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(P95Window())
        .key_by(lambda x: x["service"])
        .process(BreachDetector())
        .map(lambda x: json.dumps(x), Types.STRING())
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(OUTPUT_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    alerts.sink_to(sink)

    print("[JOB] Submitting job to Flink cluster")
    env.execute("latency-slo-monitor")


if __name__ == "__main__":
    main()
