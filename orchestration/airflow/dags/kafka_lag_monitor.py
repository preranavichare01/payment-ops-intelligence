"""
Monitors Kafka consumer group lag for the streaming jobs. High lag means
the Spark consumers can't keep up with producer throughput — either
maxOffsetsPerTrigger is too conservative, or the job has degraded.

Why consumer lag matters: lag = (latest offset in partition) - (offset the
consumer group has committed). Growing lag means events are piling up
faster than they're processed — the exact leading indicator of a pipeline
falling behind before anyone notices in the dashboards.

Escalation logic:
- lag > 5000 on transactions -> alert only (still recoverable on its own)
- lag > 20000 -> alert AND trigger Spark restart with higher
  maxOffsetsPerTrigger, since sustained high lag means the current batch
  size can't drain the backlog fast enough on its own.

Interview question this answers: "How do you monitor and respond to
consumer lag in a Kafka-based pipeline?" — this is a concrete, working
answer with real thresholds and a real remediation action, not just
"we'd set up an alert."
"""

import subprocess
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

PROJECT_ROOT = Path("/opt/airflow/project")

KAFKA_CONTAINER_NAME = "kafka"
CONSUMER_GROUP_TRANSACTIONS = "spark-kafka-source-transactions"  # Spark auto-generates group IDs;
CONSUMER_GROUP_SETTLEMENTS = "spark-kafka-source-settlements"    # adjust to match actual group.id if set explicitly

LAG_ALERT_THRESHOLD = 5000
LAG_CRITICAL_THRESHOLD = 20000

default_args = {
    "owner": "prerana",
    "retries": 0,
    "start_date": datetime(2026, 7, 1),
}


def _get_consumer_group_lag(group_id: str) -> int:
    """
    Shells out to the Kafka container's built-in consumer-group tool since
    we're on local Docker Kafka (no Confluent Cloud API here). Sums lag
    across all partitions for the group.
    """
    try:
        result = subprocess.run(
            [
                "docker", "exec", KAFKA_CONTAINER_NAME,
                "kafka-consumer-groups", "--bootstrap-server", "localhost:9092",
                "--describe", "--group", group_id,
            ],
            capture_output=True, text=True, timeout=30,
        )
        output = result.stdout

        total_lag = 0
        for line in output.splitlines():
            # Line format: TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
            match = re.match(r"\S+\s+\d+\s+\d+\s+\d+\s+(\d+)", line.strip())
            if match:
                total_lag += int(match.group(1))
        return total_lag
    except Exception as e:
        print(f"[LAG CHECK FAILED for {group_id}] {e}")
        return -1  # sentinel: could not determine lag


def _send_slack_alert(message: str):
    try:
        hook = SlackWebhookHook(slack_webhook_conn_id="slack_webhook")
        hook.send(text=message)
    except Exception as e:
        print(f"[SLACK ALERT - not configured or failed: {e}] {message}")


def _restart_with_increased_parallelism(stream_script_name: str):
    """
    Relaunches the stream with a higher maxOffsetsPerTrigger via env var
    override, so the script can read it and widen its backpressure cap
    without needing a code change per incident.
    """
    import subprocess as sp
    script_path = PROJECT_ROOT / "streaming" / f"{stream_script_name}.py"
    log_path = PROJECT_ROOT / f"{stream_script_name}_lag_restart.log"

    env = {"MAX_OFFSETS_PER_TRIGGER": "5000"}  # 5x default of 1000
    sp.Popen(
        ["python", str(script_path)],
        stdout=open(log_path, "a"),
        stderr=sp.STDOUT,
        env={**__import__("os").environ, **env},
        start_new_session=True,
    )
    print(f"[LAG-TRIGGERED RESTART] Relaunched {stream_script_name} with widened maxOffsetsPerTrigger")


def check_kafka_lag(**context):
    now = datetime.now(timezone.utc).isoformat()

    groups_to_streams = {
        CONSUMER_GROUP_TRANSACTIONS: "transaction_stream",
        CONSUMER_GROUP_SETTLEMENTS: "settlement_stream",
    }

    for group_id, stream_name in groups_to_streams.items():
        lag = _get_consumer_group_lag(group_id)
        print(f"[{now}] group={group_id} lag={lag}")

        if lag < 0:
            continue  # could not determine, skip rather than false-alarm

        if lag > LAG_CRITICAL_THRESHOLD:
            _send_slack_alert(
                f":rotating_light: CRITICAL Kafka lag on `{group_id}`: {lag} messages. "
                f"Restarting `{stream_name}` with increased parallelism."
            )
            _restart_with_increased_parallelism(stream_name)
        elif lag > LAG_ALERT_THRESHOLD:
            _send_slack_alert(
                f":warning: Elevated Kafka lag on `{group_id}`: {lag} messages."
            )


with DAG(
    dag_id="kafka_lag_monitor",
    default_args=default_args,
    schedule_interval=timedelta(minutes=2),
    catchup=False,
    tags=["monitoring", "kafka"],
) as dag:

    check_lag_task = PythonOperator(
        task_id="check_kafka_lag",
        python_callable=check_kafka_lag,
    )