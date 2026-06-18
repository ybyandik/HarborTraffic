import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from ingestion.harbor_config import WEATHER_HARBORS, load_harbor_config
from ingestion.publisher import publish

ROOT = Path(__file__).resolve().parent.parent

def _get(url, query, api_cfg):
    for attempt in range(api_cfg["retry_attempts"]):
        try:
            resp = requests.get(url, params=query, timeout=api_cfg["timeout"])
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            if attempt < api_cfg["retry_attempts"] - 1:
                time.sleep(api_cfg["retry_delay"])
            else:
                print(f"Open-Meteo API failed ({url}): {exc}", file=sys.stderr)
                return None

def fetch_weather(cfg):
    loc = cfg["location"]
    api = cfg["api"]

    base_query = {
        "latitude": loc["latitude"],
        "longitude": loc["longitude"],
        "timezone": loc["timezone"],
        "forecast_days": 1,
    }

    forecast = _get(
        api["forecast_url"],
        {**base_query, "current": ",".join(cfg["weather_params"])},
        api,
    )
    if not forecast:
        return None

    marine = _get(
        api["marine_url"],
        {**base_query, "current": ",".join(cfg["marine_params"])},
        api,
    )
    if marine:
        forecast.setdefault("current", {}).update(marine.get("current", {}))

    return forecast

def transform(api_data, location_name):
    current = api_data.get("current", {})
    ts = current.get("time")
    if not ts:
        return None

    timestamp = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    is_day = current.get("is_day")
    if is_day == 1:
        day_or_night = "day"
    elif is_day == 0:
        day_or_night = "night"
    else:
        day_or_night = None

    return {
        "stream": "weather",
        "location": location_name,
        "timestamp": timestamp.astimezone(timezone.utc).isoformat(),
        "wave_height": current.get("wave_height"),
        "temperature_2m": current.get("temperature_2m"),
        "precipitation": current.get("precipitation", 0.0),
        "weather_code": current.get("weather_code"),
        "visibility": current.get("visibility"),
        "wind_speed_10m": current.get("wind_speed_10m"),
        "day_or_night": day_or_night,
        "wind_gusts_10m": current.get("wind_gusts_10m"),
    }

def run_once(harbor_id=None, config_path=None):
    cfg = load_harbor_config(harbor_id=harbor_id, config_path=config_path)
    raw = fetch_weather(cfg)
    if not raw:
        return 1

    record = transform(raw, cfg["location"]["name"])
    if not record:
        print("Weather transform failed", file=sys.stderr)
        return 1

    publish(record, "weather", cfg=cfg)
    print(
        f"Weather: {record['location']} {record['day_or_night']} "
        f"T={record['temperature_2m']}°C wave={record['wave_height']}m "
        f"wind={record['wind_speed_10m']} gusts={record['wind_gusts_10m']} "
        f"code={record['weather_code']} vis={record['visibility']}m",
        file=sys.stderr,
    )
    return 0

def main():
    import argparse

    parser = argparse.ArgumentParser(description="which harbor weather")
    parser.add_argument(
        "--harbor",
        choices=sorted(WEATHER_HARBORS),
        help=".",
    )
    parser.add_argument(
        "--config",
        help=".",
    )
    parser.add_argument("--test-api", action="store_true", help=".")
    args = parser.parse_args()

    if args.config and args.harbor:
        print("Use either --harbor or --config, not both", file=sys.stderr)
        return 1

    if args.test_api:
        cfg = load_harbor_config(harbor_id=args.harbor, config_path=args.config)
        raw = fetch_weather(cfg)
        if raw and "current" in raw:
            record = transform(raw, cfg["location"]["name"])
            print("API connection OK", json.dumps(record, ensure_ascii=False), file=sys.stderr)
            return 0
        return 1

    return run_once(harbor_id=args.harbor, config_path=args.config)

if __name__ == "__main__":
    raise SystemExit(main())
