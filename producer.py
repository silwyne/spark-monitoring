import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer


# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC_NAME = "first-topic"


# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def generate_event():
    """
    Generate random event data
    """
    return {
        "event_id": random.randint(100000, 999999),
        "user_id": random.randint(1, 10000),
        "event_type": random.choice([
            "login",
            "purchase",
            "logout",
            "page_view"
        ]),
        "amount": round(random.uniform(10, 500), 2),
        "timestamp": datetime.utcnow().isoformat()
    }


def main():
    print(f"Producing messages to topic: {TOPIC_NAME}")

    try:
        while True:
            event = generate_event()

            producer.send(
                TOPIC_NAME,
                value=event
            )

            producer.flush()

            print("Sent:", event)

            # Generate one event every second
            # time.sleep(0.1)

    except KeyboardInterrupt:
        print("Stopping producer...")

    finally:
        producer.close()


if __name__ == "__main__":
    main()