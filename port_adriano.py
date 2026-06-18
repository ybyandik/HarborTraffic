#!/usr/bin/env python3
"""
Source webcam:
    https://www.webcamtaxi.com/en/spain/balearic-islands/port-adriano-cam.html

Usage:
    python port_adriano.py
    python port_adriano.py --preview-zones data/port_adriano_frame.jpg

Kafka + Spark:
    python port_adriano.py
    python -m spark.harbor_analytics --console

Weather ingest:
    python -m ingestion.weather
"""
from port_adriano.live import main

if __name__ == "__main__":
    raise SystemExit(main())
