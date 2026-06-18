import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

def event_log_dir():
    raw = os.getenv("EVENT_LOG_DIR")
    if raw:
        return Path(raw)
    return ROOT / "data" / "events"
