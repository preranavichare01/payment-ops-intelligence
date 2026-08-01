"""
Checks if the Spark Streaming jobs (transaction_stream.py, settlement_stream.py)
are still alive by inspecting Delta checkpoint freshness. If the latest
checkpoint commit is older than 8 minutes, the streaming job is presumed
stalled/crashed — alert and attempt an automatic restart.

Why checkpoint freshness, not a heartbeat: Spark Structured Streaming writes
a new checkpoint entry on every successful micro-batch. If checkpoints stop
advancing, the job is either crashed, stuck, or disconnected from Kafka —
all of which look identical from the outside, so "last commit time" is the
simplest reliable proxy for "is this job actually doing work."

Why 8 minutes: micro-batches trigger roughly every few seconds under normal
load, so 8 minutes of silence is many multiples of the expected cadence —
long enough to avoid false positives from a single slow batch, short enough
that ops isn't blind to a real outage for too long.

Interview question this answers: "How do you build a self-healing streaming
pipeline?" — Airflow polls checkpoint metadata rather than the Spark process
itself, and triggers a restart action when staleness is detected.
"""

import os
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

PROJECT_ROOT = Path("/opt/airflow/project")
CHECKPOINT_ROOT = PROJECT_ROOT / "data" / "checkpoints"
MONITORING_LOG_PATH = PROJECT_ROOT / "data" / "processed" / "monitoring_checkpoint_health"

STALE_THRESHOLD_MINUTES = 8

STREAMS_TO_MONITOR = {
    "transaction_stream": "silver_transactions",
    "settlement_stream": "silver_settlements",
}

default_args = {
    "owner": "prerana",
    "retries": 0,
    "start_date": datetime(2026, 7, 1),
}


def _get_latest_checkpoint_time(checkpoint_dir: Path):
    offsets_dir = checkpoint_dir / "offsets"
    if not offsets_dir.exists():
        return None

    files = list(offsets_dir.glob("*"))
    if not files:
        return None

    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    return datetime.fromtimestamp(latest_file.stat().st_mtime, tz=timezone.utc)


def _send_slack_alert(message: str):
    try:
        hook = SlackWebhookHook(slack_webhook_conn_id="slack_webhook")
        hook.send(text=message)
    except Exception as e:
        print(f"[SLACK ALERT - not configured or failed: {e}] {message}")


def _restart_streaming_job(stream_script_name: str):
    script_path = PROJECT_ROOT / "streaming" / f"{stream_script_name}.py"
    log_path = PROJECT_ROOT / f"{stream_script_name}_auto_restart.log"

    subprocess.Popen(
        ["python", str(script_path)],
        stdout=open(log_path, "a"),
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    print(f"[AUTO-RESTART] Relaunched {stream_script_name}")


def check_streaming_health(**context):
    now = datetime.now(timezone.utc)
    results = []

    for stream_name, silver_table in STREAMS_TO_MONITOR.items():
        checkpoint_dir = CHECKPOINT_ROOT / silver_table
        latest_commit = _get_latest_checkpoint_time(checkpoint_dir)

        if latest_commit is None:
            status = "NO_CHECKPOINT_FOUND"
            age_minutes = None
        else:
            age_minutes = (now - latest_commit).total_seconds() / 60
            status = "STALE" if age_minutes > STALE_THRESHOLD_MINUTES else "HEALTHY"

        results.append({
            "stream_name": stream_name,
            "status": status,
            "age_minutes": age_minutes,
            "checked_at": now.isoformat(),
        })

        print(f"[{stream_name}] status={status} age_minutes={age_minutes}")

        if status in ("STALE", "NO_CHECKPOINT_FOUND"):
            _send_slack_alert(
                f":rotating_light: Streaming job `{stream_name}` appears "
                f"{status} (last checkpoint {age_minutes} min ago). "
                f"Attempting automatic restart."
            )
            _restart_streaming_job(stream_name)

    MONITORING_LOG_PATH.mkdir(parents=True, exist_ok=True)
    log_file = MONITORING_LOG_PATH / f"check_{now.strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_file, "w") as f:
        json.dump(results, f, indent=2)


with DAG(
    dag_id="streaming_health_monitor",
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=["monitoring", "streaming"],
) as dag:

    check_health_task = PythonOperator(
        task_id="check_streaming_health",
        python_callable=check_streaming_health,
    )