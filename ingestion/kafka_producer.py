import json
import sys
from datetime import datetime, timezone

from ingestion.kafka_config import kafka_settings
from confluent_kafka import Producer

_producers = {}

def _delivery_callback(err, msg):
    if err:
        print(f"Kafka delivery error: {err}", file=sys.stderr)
    else:
        print(
            f" Kafka delivered {msg.topic()}[{msg.partition()}] @{msg.offset()}", file=sys.stderr)

class EventProducer:
    def __init__(self, bootstrap_servers="localhost:9092", topic="port-adriano-events", client_id="port-adriano-ingest"):

        self.topic = topic
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "acks": "all",
            "retries": 3,
            "linger.ms": 50,
        })
        print(f"Kafka producer ready: {bootstrap_servers} topic={topic}", file=sys.stderr)

    def publish(self, record, key=None):
        if "timestamp" not in record:
            record = {**record, "timestamp": datetime.now(timezone.utc).isoformat()}
        payload = json.dumps(record, ensure_ascii=False).encode("utf-8")
        k = key or record.get("location") or record.get("stream")
        self._producer.produce(
            self.topic,
            payload,
            key=str(k).encode("utf-8") if k is not None else None,
            callback=_delivery_callback,
        )
        self._producer.poll(0)

    def flush(self, timeout=10.0):
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            print(f"Kafka flush: {remaining} messages still pending", file=sys.stderr)

def get_producer(topic):
    if topic in _producers:
        return _producers[topic]
    ks = kafka_settings()
    p = EventProducer(
        bootstrap_servers=ks["bootstrap_servers"],
        topic=topic,
        client_id=ks["client_id"],
    )
    _producers[topic] = p
    return p

def publish_to_kafka(record, stream, cfg=None):
    ks = kafka_settings(cfg=cfg)
    if stream == "ships":
        topic = ks["ship_topic"]
    elif stream == "weather":
        topic = ks["weather_topic"]
    else:
        raise ValueError(f"Unknown stream: {stream}")
    get_producer(topic).publish(record)

def flush_all():
    for p in _producers.values():
        p.flush()
