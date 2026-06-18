python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

sudo apt install python3-gi gstreamer1.0-plugins-good gstreamer1.0-plugins-bad gstreamer1.0-libav

mkdir -p models

docker compose up -d
python -m ingestion.kafka_setup

Airflow UI: http://localhost:8080 — `airflow` / `airflow`  
Enable DAG: `port_adriano_weather_ingest`

python port_adriano.py
python -m ingestion.weather

# streaming spark
python -m spark.harbor_analytics --console

# batch analysis
python -m spark.batch_analytics

python -m ml.weather_suitability

python -m ingestion.kafka_consumer --topic port-adriano-weather-events
