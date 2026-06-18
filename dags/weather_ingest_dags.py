from datetime import datetime, timedelta
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

from airflow import DAG
from airflow.operators.python import PythonOperator

from ingestion.harbor_config import WEATHER_HARBORS, harbor_location_name


def _poll_interval():
    try:
        import yaml

        cfg_path = ROOT / "config" / "config.yaml"
        with open(cfg_path, encoding="utf-8") as f:
            seconds = int(yaml.safe_load(f).get("weather_poll_interval", 600))
        return timedelta(seconds=max(seconds, 60))
    except Exception:
        return timedelta(minutes=10)


def _make_fetch_weather(harbor_id):
    def fetch_and_publish_weather(**context):
        from ingestion.weather import run_once

        if run_once(harbor_id=harbor_id) != 0:
            raise RuntimeError(f"weather ingest failed for {harbor_id}")

    return fetch_and_publish_weather


def flush_kafka_producer(**context):
    from ingestion.publisher import flush

    flush()


def _make_verify_log(harbor_id, location_name):
    def verify_weather_log(**context):
        from ingestion.paths import event_log_dir

        log_path = event_log_dir() / "weather.jsonl"
        if not log_path.exists():
            raise FileNotFoundError(f"missing weather log: {log_path}")

        last_record = None
        with log_path.open(encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                record = json.loads(line)
                if record.get("location") == location_name:
                    last_record = record

        if not last_record:
            raise ValueError(f"no weather record for {location_name!r} in {log_path}")

        required = ("location", "timestamp", "temperature_2m", "wave_height")
        missing = [k for k in required if last_record.get(k) is None]
        if missing:
            raise ValueError(f"weather record for {location_name} missing fields: {missing}")

        context["ti"].xcom_push(key="last_weather", value=last_record)
        print(
            f"Finished ingesting weather for {last_record['location']} @ {last_record['timestamp']} "
            f"T={last_record['temperature_2m']}°C wave={last_record['wave_height']}m",
        )

    return verify_weather_log


default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 1, 1),
}

for harbor_id in WEATHER_HARBORS:
    location_name = harbor_location_name(harbor_id)
    dag_id = f"{harbor_id}_weather_ingest"

    dag = DAG(
        dag_id=dag_id,
        default_args={**default_args, "owner": harbor_id},
        description=f"Weather ingestion for {location_name}",
        schedule=_poll_interval(),
        catchup=False,
        max_active_runs=1,
        tags=[harbor_id, "weather", "ingest", "kafka"],
    )

    fetch_weather = PythonOperator(
        task_id="fetch_and_publish_weather",
        python_callable=_make_fetch_weather(harbor_id),
        dag=dag,
    )

    flush_kafka = PythonOperator(
        task_id="flush_kafka",
        python_callable=flush_kafka_producer,
        dag=dag,
    )

    verify_log = PythonOperator(
        task_id="verify_weather_log",
        python_callable=_make_verify_log(harbor_id, location_name),
        dag=dag,
    )

    fetch_weather >> flush_kafka >> verify_log
    globals()[dag_id] = dag
