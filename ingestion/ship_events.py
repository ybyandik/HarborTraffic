import sys
from datetime import datetime, timezone

from ingestion.harbor_config import default_harbor_id, load_harbor_config
from ingestion.publisher import flush, publish

def publish_ship_event(
    event,
    track_id,
    enter_total,
    exit_total,
    zone_from,
    zone_to,
    location=None,
    harbor_id=None,
    cfg=None,
):
    if cfg is None:
        cfg = load_harbor_config(harbor_id=harbor_id or default_harbor_id())
    if location is None:
        location = cfg["location"]["name"]

    record = {
        "location": location,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event": event,
        "track_id": track_id,
        "zone_from": zone_from,
        "zone_to": zone_to,
        "enter_total": enter_total,
        "exit_total": exit_total,
    }

    publish(record, "ships", cfg=cfg)
    print(
        f"ship event: {event} #{track_id} ({zone_from}->{zone_to}) "
        f"in={enter_total} out={exit_total} [{location}]",
        file=sys.stderr,
    )


def flush_ship_events():
    flush()
