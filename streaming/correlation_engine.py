"""
Cross-bank failure correlation engine.

Runs as a standalone batch job (not a continuous stream) — queries the last
5 minutes of Silver Delta on a schedule (every 1 minute, via Airflow later)
and classifies whether any failure pattern seen is bank-side, NPCI-wide, or
platform-side.

Why batch, not streaming: correlation across banks is a *point-in-time*
judgment ("in this window, how many banks are unhealthy simultaneously?"),
not a per-event computation. A lightweight scheduled batch read is simpler,
cheaper, and easier to reason about than maintaining more streaming state
just for this. This is also exactly how you'd explain the choice in an
interview: not every problem needs a streaming solution just because the
upstream data is streaming.

Interview question this answers: "How do you distinguish infrastructure-wide
outages from a single vendor's problem?" — the answer is precisely this
same-window, multi-entity correlation logic.
"""

import os


import sys

from pathlib import Path
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit


# Windows PySpark spawns a Python worker subprocess for operations like
# createDataFrame() — it defaults to looking for a binary literally named
# "python3", which doesn't exist on Windows (only python.exe does).
# Pointing both these env vars at sys.executable fixes it.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def to_spark_path(p: Path) -> str:
    """Windows paths need file:/// + forward slashes for Spark's Hadoop FS layer."""
    return "file:///" + str(p).replace("\\", "/")


SILVER_PATH = to_spark_path(PROJECT_ROOT / "data" / "processed" / "silver_transactions")
GOLD_CORRELATION_PATH = to_spark_path(PROJECT_ROOT / "data" / "processed" / "gold_correlation_events")

# --- Thresholds — tunable, but these are the spec's starting values ---
BANK_FAILURE_RATE_THRESHOLD = 20.0     # % failure rate that counts as "unhealthy" for a bank
MIN_BANKS_FOR_NPCI_WIDE = 2            # 2+ banks unhealthy simultaneously = NPCI-side issue
HIGH_RESPONSE_TIME_MS = 3000           # response time above this + high failure = platform issue

BANK_METHODS = ["upi_hdfc", "upi_axis", "upi_sbi", "upi_icici"]


def build_spark():
    builder = (
        SparkSession.builder
        .appName("CorrelationEngine")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_latest_window_metrics(spark):
    """
    Reads Silver Delta and keeps only the most recent 5-minute window per
    payment_method. Since Silver has overlapping sliding windows (5min/1min
    slide), we want the LATEST window end-time per payment_method to get
    the freshest read on each bank's health — not an average across stale
    overlapping windows.
    """
    silver = spark.read.format("delta").load(SILVER_PATH)

    # window is a struct {start, end} — get the max window.end per payment_method,
    # then filter down to only those latest rows.
    latest_per_method = (
        silver
        .filter(col("payment_method").isin(BANK_METHODS))
        .groupBy("payment_method")
        .agg({"window": "max"})
        .withColumnRenamed("max(window)", "latest_window")
    )

    latest_rows = (
        silver
        .join(latest_per_method, on="payment_method")
        .filter(col("window") == col("latest_window"))
        .select(
            "payment_method",
            "window",
            "total_transactions",
            "success_rate_pct",
            "avg_response_time_ms",
            "timeout_rate_pct",
        )
    )
    return latest_rows


def classify(rows: list[dict]) -> dict:
    """
    Applies the correlation rules to the latest per-bank snapshot.

    Rule order matters: NPCI-wide check happens first because it's the more
    severe/systemic classification — if 2+ banks are down together, that's
    NOT "two independent bank problems," it's almost certainly a shared
    upstream dependency (NPCI switch, UPI rail) failing.
    """
    failing_banks = [
        r for r in rows
        if (100 - r["success_rate_pct"]) >= BANK_FAILURE_RATE_THRESHOLD
    ]
    high_latency_and_failing = [
        r for r in failing_banks
        if r["avg_response_time_ms"] >= HIGH_RESPONSE_TIME_MS
    ]

    if len(failing_banks) >= MIN_BANKS_FOR_NPCI_WIDE:
        classification = "NPCI_SIDE_ISSUE"
        affected = [r["payment_method"] for r in failing_banks]
    elif len(high_latency_and_failing) >= 1 and len(failing_banks) == 1:
        classification = "PLATFORM_SIDE_ISSUE"
        affected = [r["payment_method"] for r in high_latency_and_failing]
    elif len(failing_banks) == 1:
        classification = "BANK_SIDE_ISSUE"
        affected = [failing_banks[0]["payment_method"]]
    else:
        classification = "HEALTHY"
        affected = []

    return {
        "classification": classification,
        "affected_banks": affected,
        "detected_at": datetime.now(timezone.utc).isoformat(),
        "snapshot": rows,
    }


def send_slack_alert(result: dict):
    """
    Sends a Slack alert if a real issue is detected. No-op (just prints) if
    SLACK_WEBHOOK_URL isn't configured yet — don't block the pipeline on an
    optional integration.
    """
    if result["classification"] == "HEALTHY":
        return

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    message = (
        f":rotating_light: *{result['classification']}* detected\n"
        f"Affected: {', '.join(result['affected_banks'])}\n"
        f"Time: {result['detected_at']}"
    )

    if not webhook_url or webhook_url.startswith("your_"):
        print(f"[SLACK ALERT - not configured, printing instead]\n{message}")
        return

    import requests
    try:
        requests.post(webhook_url, json={"text": message}, timeout=5)
    except Exception as e:
        print(f"[SLACK ALERT FAILED] {e}")


def write_to_gold(spark, result: dict):
    """
    Appends this correlation check's result to Gold Delta, one row per run —
    gives you a full audit trail of every classification decision over time,
    which is exactly the kind of history JP Morgan-style compliance review
    would expect to exist.
    """
    row = spark.createDataFrame([{
        "classification": result["classification"],
        "affected_banks": ",".join(result["affected_banks"]) if result["affected_banks"] else "",
        "detected_at": result["detected_at"],
    }])

    (
        row.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(GOLD_CORRELATION_PATH)
    )


def run_once():
    spark = build_spark()

    latest_df = get_latest_window_metrics(spark)
    rows = [r.asDict() for r in latest_df.collect()]

    if not rows:
        print("No Silver data available yet for correlation check — skipping this run.")
        spark.stop()
        return

    result = classify(rows)
    print(f"[{result['detected_at']}] Classification: {result['classification']} | Affected: {result['affected_banks']}")

    write_to_gold(spark, result)
    send_slack_alert(result)

    spark.stop()


if __name__ == "__main__":
    run_once()