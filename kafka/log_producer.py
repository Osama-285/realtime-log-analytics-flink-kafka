import json
import random
import time
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9094",
    "acks": "all",
    "linger.ms": 50,
    "retries": 3,
}

TOPIC = "app_logs_raw"
producer = Producer(KAFKA_CONFIG)

SERVICES = {
    "auth-service": {"base_latency": 120, "error_rate": 0.30},
    "payment-service": {"base_latency": 300, "error_rate": 0.02},
    "order-service": {"base_latency": 180, "error_rate": 0.015},
    "notification-service": {"base_latency": 80, "error_rate": 0.005},
}

HOSTS = ["node-1", "node-2", "node-3"]

ERROR_MESSAGES = [
    "Database timeout",
    "Null pointer exception",
    "Upstream service unavailable",
    "Connection refused",
    "Request validation failed",
]

INFO_MESSAGES = [
    "Request processed successfully",
    "User authenticated",
    "Event published",
    "Cache hit",
    "Operation completed",
]


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery Failed: {err}")


def generate_log(service_name, config):
    now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    # now_iso = datetime.utcnow().replace(microsecond=0).isoformat()
    spike = int(time.time()) % 60 < 5
    error_rate = config["error_rate"]
    if spike:
        error_rate *= 10

    is_error = random.random() < error_rate

    level = "ERROR" if is_error else "INFO"
    message = random.choice(ERROR_MESSAGES if is_error else INFO_MESSAGES)

    latency = int(random.gauss(config["base_latency"] * (3 if spike else 1), 30))

    latency = max(latency, 10)

    return {
        "timestamp": now_iso,
        "service": service_name,
        "host": random.choice(HOSTS),
        "level": level,
        "request_id": f"req-{uuid.uuid4().hex[:8]}",
        "message": message,
        "latency_ms": latency,
    }


def main():
    print("Starting log producer...")
    while True:
        for service, config in SERVICES.items():
            events_per_sec = random.randint(5, 20)

            for _ in range(events_per_sec):
                event = generate_log(service, config)
                print("Event: ", event)
                producer.produce(
                    topic=TOPIC,
                    key=event["service"],
                    value=json.dumps(event),
                    on_delivery=delivery_report,
                )
        producer.poll(0)
        time.sleep(1)


if __name__ == "__main__":
    main()
