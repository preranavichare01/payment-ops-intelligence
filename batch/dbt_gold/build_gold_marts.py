"""
Materializes the 3 Gold mart tables via direct Spark writes, bypassing
dbt's incremental/table materialization due to a known dbt-spark 1.10.3 +
Delta V2 catalog incompatibility (CREATE OR REPLACE TABLE requests a
TRUNCATE capability that isn't supported on managed spark_catalog tables
in this version combination — documented in SYSTEM_DESIGN.md).

dbt still owns the view-layer transformation logic (staging + intermediate
models); this script just handles the final Delta materialization step
that dbt-spark can't currently do reliably here.
"""

import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages io.delta:delta-spark_2.12:3.2.0 "
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
    "pyspark-shell"
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def to_spark_path(p: Path) -> str:
    return "file:///" + str(p).replace("\\", "/")


BRONZE_TXN = to_spark_path(PROJECT_ROOT / "data" / "processed" / "bronze_transactions")
SILVER_SETTLEMENTS = to_spark_path(PROJECT_ROOT / "data" / "processed" / "silver_settlements")
GOLD_CORRELATION = to_spark_path(PROJECT_ROOT / "data" / "processed" / "gold_correlation_events")

MART_OPS_PATH = to_spark_path(PROJECT_ROOT / "data" / "processed" / "mart_ops_dashboard_metrics")
MART_RECON_PATH = to_spark_path(PROJECT_ROOT / "data" / "processed" / "mart_settlement_reconciliation")
MART_CORR_PATH = to_spark_path(PROJECT_ROOT / "data" / "processed" / "mart_correlation_summary")
SILVER_PAYOUTS = to_spark_path(PROJECT_ROOT / "data" / "processed" / "silver_payouts")
MART_PAYOUT_PATH = to_spark_path(PROJECT_ROOT / "data" / "processed" / "mart_payout_health")
TXN_SCHEMA = (
    "transaction_id STRING, user_id STRING, merchant_id STRING, merchant_category STRING, "
    "payment_method STRING, amount_inr DOUBLE, currency STRING, status STRING, "
    "bank_response_code STRING, response_time_ms INT, is_international BOOLEAN, "
    "device_type STRING, city STRING, timestamp STRING"
)

spark = (
    SparkSession.builder
    .appName("BuildGoldMarts")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

from pyspark.sql.functions import from_json, col, count, sum as ssum, avg, when, date_trunc

print("Reading Bronze transactions...")
bronze_raw = spark.read.format("delta").load(BRONZE_TXN)
txns = (
    bronze_raw
    .select(from_json(col("value"), TXN_SCHEMA).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("timestamp").cast("timestamp"))
    .withColumn("event_date", col("event_time").cast("date"))
    .filter(col("transaction_id").isNotNull())
)

print("Reading Silver settlements...")
settlements = spark.read.format("delta").load(SILVER_SETTLEMENTS)

# --- mart_ops_dashboard_metrics ---
print("Building mart_ops_dashboard_metrics...")
ops_metrics = (
    txns.groupBy("event_date", "payment_method", "merchant_category")
    .agg(
        count("*").alias("total_transactions"),
        ssum(when(col("status") == "success", 1).otherwise(0)).alias("success_count"),
        ssum(when(col("status") == "timeout", 1).otherwise(0)).alias("timeout_count"),
        ssum("amount_inr").alias("total_volume_inr"),
        avg("response_time_ms").alias("avg_response_time_ms"),
    )
)
ops_metrics.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(MART_OPS_PATH)
print(f"  wrote {ops_metrics.count()} rows")

# --- mart_settlement_reconciliation ---
print("Building mart_settlement_reconciliation...")
successful_txns = txns.filter(col("status") == "success")
enriched = successful_txns.alias("t").join(
    settlements.alias("s"),
    on=col("t.transaction_id") == col("s.transaction_id"),
    how="left_outer",
).select(
    col("t.merchant_id"),
    col("t.amount_inr"),
    col("t.event_date"),
    col("s.settled_amount_inr"),
    (col("s.delay_seconds") > 180).alias("is_delayed"),
)

recon = (
    enriched.groupBy("event_date", "merchant_id")
    .agg(
        count("*").alias("total_settled_transactions"),
        ssum(when(col("is_delayed"), 1).otherwise(0)).alias("delayed_count"),
        ssum("amount_inr").alias("total_txn_amount"),
        ssum("settled_amount_inr").alias("total_settled_amount"),
    )
    .withColumn("amount_discrepancy", col("total_txn_amount") - col("total_settled_amount"))
)
recon.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(MART_RECON_PATH)
print(f"  wrote {recon.count()} rows")

# --- mart_correlation_summary ---
print("Building mart_correlation_summary...")
correlation = spark.read.format("delta").load(GOLD_CORRELATION)
correlation.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(MART_CORR_PATH)
print(f"  wrote {correlation.count()} rows")
# --- mart_payout_health ---
print("Building mart_payout_health...")
payouts = spark.read.format("delta").load(SILVER_PAYOUTS)

payout_health = (
    payouts
    .withColumn("event_date", col("expected_payout_time").cast("timestamp").cast("date"))
    .groupBy("event_date", "platform", "recipient_type")
    .agg(
        count("*").alias("total_payouts"),
        ssum(when(col("payout_status") == "delayed", 1).otherwise(0)).alias("delayed_count"),
        ssum(when(col("payout_status") == "paid", 1).otherwise(0)).alias("paid_count"),
        ssum("payout_amount_inr").alias("total_payout_volume_inr"),

        avg("delay_minutes").alias("avg_delay_minutes"),
    )
)
payout_health.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(MART_PAYOUT_PATH)
print(f"  wrote {payout_health.count()} rows")
print("All Gold marts built successfully.")
spark.stop()