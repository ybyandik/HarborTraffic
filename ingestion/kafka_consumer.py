import argparse
import json
import sys

from ingestion.kafka_config import kafka_settings

def consume(topics, timeout=10.0):
    from confluent_kafka import Consumer, KafkaException

    ks = kafka_settings()
    consumer = Consumer({
        "bootstrap.servers": ks["bootstrap_servers"],
        "group.id": "port-adriano-debug-consumer",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe(topics)
    print(f"Listening {topics} on {ks['bootstrap_servers']}", file=sys.stderr)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            record = json.loads(msg.value().decode("utf-8"))
            print(
                f"{msg.topic()}[{msg.partition()}] "
                f"{json.dumps(record, ensure_ascii=False)}",
                flush=True,
            )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def main():
    ks = kafka_settings()
    parser = argparse.ArgumentParser(description="Kafka debug consumer")
    parser.add_argument(
        "topics",
        nargs="*",
        default=[ks["ship_topic"], ks["weather_topic"]],
        help=".",
    )
    args = parser.parse_args()
    consume(args.topics)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
