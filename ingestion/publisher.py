import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from ingestion.kafka_config import load_project_config
from ingestion.kafka_producer import flush_all, publish_to_kafka
from ingestion.paths import event_log_dir

EVENT_DIR = event_log_dir()

def publish(record, stream, cfg=None):
    if "timestamp" not in record:
        record = {**record, "timestamp": datetime.now(timezone.utc).isoformat()}
    record.setdefault("stream", stream)

    log_path = EVENT_DIR / f"{stream}.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        log_path.parent.chmod(0o777)
    except OSError:
        pass
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
    try:
        log_path.chmod(0o666)
    except OSError:
        pass

    if cfg is None:
        cfg = load_project_config()
    try:
        publish_to_kafka(record, stream, cfg=cfg)
    except Exception as exc:
        print(f"Kafka publish failed: {exc}", file=sys.stderr)

def flush():
    flush_all()
