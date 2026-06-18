import os
from pathlib import Path

from ingestion.harbor_config import default_harbor_id, load_harbor_config

ROOT = Path(__file__).resolve().parent.parent


def spark_settings(harbor_id=None):
    hid = harbor_id or os.getenv("HARBOR", default_harbor_id())
    cfg = load_harbor_config(harbor_id=hid)
    k = cfg.get("kafka", {})
    s = cfg.get("spark", {})
    data = ROOT / "data"
    return {
        "harbor_id": hid,
        "location": cfg["location"]["name"],
        "bootstrap_servers": os.getenv(
            "KAFKA_BOOTSTRAP", k.get("bootstrap_servers", "localhost:9092"),
        ),
        "ship_topic": os.getenv("KAFKA_SHIP_TOPIC", k.get("ship_topic", "port-adriano-ship-events")),
        "weather_topic": os.getenv(
            "KAFKA_WEATHER_TOPIC", k.get("weather_topic", "port-adriano-weather-events"),
        ),
        "checkpoint_dir": ROOT / s.get("checkpoint_dir", f"data/spark/checkpoints/{hid}"),
        "parquet_dir": ROOT / s.get("parquet_dir", f"data/parquet/{hid}"),
        "trigger_seconds": int(os.getenv("SPARK_TRIGGER_SEC", s.get("trigger_seconds", 30))),
        "ships_jsonl": data / "events" / "ships.jsonl",
        "weather_jsonl": data / "events" / "weather.jsonl",
    }
