import json
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction
from pyflink.common import Types
from pyflink.common.time import Time
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
)
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import RuntimeContext


KAFKA_BROKERS = "broker2:29094"
INPUT_TOPIC = "incident_alerts"
OUTPUT_TOPIC = "incident_escalations"


class IncidentAggregator(KeyedProcessFunction):

    def open(self, ctx: RuntimeContext):
        print("[OPEN] Operator opened, initializing state...")

        ttl_config = (
            StateTtlConfig
            .new_builder(Time.hours(1))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .cleanup_full_snapshot()
            .build()
        )

        desc = ValueStateDescriptor("alert_count", Types.INT())
        desc.enable_time_to_live(ttl_config)

        self.alert_count = ctx.get_state(desc)
        print("[OPEN] State descriptor created with TTL 1 hour")

    def process_element(self, value, ctx):
        print(f"[PROCESS_ELEMENT] Received raw event: {value}")
        print(f"[PROCESS_ELEMENT] Received raw event: {self}")
        print(f"[PROCESS_ELEMENT] Received raw event: {ctx}")

        # value is a dict
        count = self.alert_count.value()
        if count is None:
            count = 0

        count += 1
        self.alert_count.update(count)
        print(f"[PROCESS_ELEMENT] Key '{ctx.get_current_key()}', updated alert_count = {count}")

        if count >= 3:
            value["severity"] = "ESCALATED"
            value["escalation_reason"] = "MULTIPLE_INCIDENTS"
            print(f"[ESCALATION] Key '{ctx.get_current_key()}' escalated due to {count} incidents")
            # Optional: reset counter
            self.alert_count.clear()
            print(f"[ESCALATION] Key '{ctx.get_current_key()}' state cleared after escalation")

        # Show final JSON before sending
        output_json = json.dumps(value)
        print(f"[OUTPUT] Event ready to sink: {output_json}")
        yield output_json


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(6)
    env.enable_checkpointing(30000)
    print("[MAIN] Execution environment created with parallelism 6 and checkpointing every 30s")

    # -------- Kafka Source --------
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics(INPUT_TOPIC)
        .set_group_id("incident-aggregator")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="incident-source"
    )
    print(f"[MAIN] Kafka source created for topic '{INPUT_TOPIC}'")

    # ---- Parse JSON ----
    parsed = stream.map(
        lambda x: json.loads(x),
        Types.MAP(Types.STRING(), Types.STRING())
    ).map(lambda x: print(f"[MAP] Parsed JSON: {x}") or x)  # Print after parse
    print("[MAIN] Added JSON parsing map function")

    # -------- Keyed Stateful Processing --------
    escalated = (
        parsed
        .key_by(lambda x: x["service"], Types.STRING())
        .process(IncidentAggregator(), Types.STRING())
    )
    print("[MAIN] Keyed stream and process function applied")

    # -------- Kafka Sink --------
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

    escalated.sink_to(sink)
    print(f"[MAIN] Kafka sink configured for topic '{OUTPUT_TOPIC}'")

    env.execute("incident-aggregator")
    print("[MAIN] Job submitted!")


if __name__ == "__main__":
    main()
