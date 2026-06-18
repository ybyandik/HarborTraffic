import os
from ingestion.harbor_config import load_harbor_config

_cfg = None

def load_project_config(config_path=None, harbor_id=None):
    global _cfg
    if config_path or harbor_id or _cfg is None:
        _cfg = load_harbor_config(harbor_id=harbor_id, config_path=config_path)
    return _cfg


def kafka_settings(config_path=None, cfg=None, harbor_id=None):
    if cfg is None:
        cfg = load_project_config(config_path, harbor_id=harbor_id)
    k = cfg.get("kafka", {})
    return {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP", k.get("bootstrap_servers", "localhost:9092")),
        "ship_topic": os.getenv("KAFKA_SHIP_TOPIC", k.get("ship_topic", "port-adriano-ship-events")),
        "weather_topic": os.getenv(
            "KAFKA_WEATHER_TOPIC", k.get("weather_topic", "port-adriano-weather-events"),
        ),
        "client_id": os.getenv("KAFKA_CLIENT_ID", k.get("client_id", "port-adriano-ingest")),
        "partitions": int(os.getenv("KAFKA_PARTITIONS", k.get("partitions", 3))),
        "replication_factor": int(os.getenv("KAFKA_REPLICATION", k.get("replication_factor", 1))),
    }
