import sys

from ingestion.harbor_config import WEATHER_HARBORS, load_harbor_config
from ingestion.kafka_config import kafka_settings

def _harbor_topics():
    topics = set()
    for harbor_id in WEATHER_HARBORS:
        cfg = load_harbor_config(harbor_id=harbor_id)
        ks = kafka_settings(cfg=cfg)
        topics.add(ks["ship_topic"])
        topics.add(ks["weather_topic"])
    return sorted(topics)

def create_topics():
    from confluent_kafka.admin import AdminClient, NewTopic

    ks = kafka_settings()
    admin = AdminClient({"bootstrap.servers": ks["bootstrap_servers"]})

    topics = [
        NewTopic(
            name,
            num_partitions=ks["partitions"],
            replication_factor=ks["replication_factor"],
        )
        for name in _harbor_topics()
    ]

    futures = admin.create_topics(topics)
    ok = True
    for name, future in futures.items():
        try:
            future.result()
            print(f"Topic ready: {name}", file=sys.stderr)
        except Exception as exc:
            if "TOPIC_ALREADY_EXISTS" in str(exc) or "already exists" in str(exc).lower():
                print(f"Topic exists: {name}", file=sys.stderr)
            else:
                print(f"Topic creation failed: {name}: {exc}", file=sys.stderr)
                ok = False
    return 0 if ok else 1

def main():
    return create_topics()

if __name__ == "__main__":
    raise SystemExit(main())
