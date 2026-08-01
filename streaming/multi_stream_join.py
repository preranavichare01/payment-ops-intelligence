"""
Joins Silver transactions with Silver settlements to detect reconciliation
issues: SETTLEMENT_DELAYED, ORPHAN_SETTLEMENT, AMOUNT_MISMATCH.

Batch job like correlation_engine.py — runs on a schedule, not continuously.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def to_spark_path(p: Path) -> str:
    return "file:///" + str(p).replace("\\", "/")


SILVER_TXN_PATH = to_spark_path(PROJECT_ROOT / "data" / "processed" / "silver_transactions")
SILVER_SETTLEMENT_PATH = to_spark_path(PROJECT_ROOT / "data" / "processed" / "silver_settlements")
GOLD_MISMATCH_PATH = to_spark_path(PROJECT_ROOT / "data" / "processed" / "gold_settlement_mismatches")

SETTLEMENT_DELAY_THRESHOLD_SECONDS = 180  # 3 minutes, per spec


def build_spark():
    builder = (
        SparkSession.builder
        .appName("MultiStreamJoin")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_successful_transactions(spark):
    """
    Bronze transactions have raw JSON, not the typed columns we need for
    this join. We re-parse Bronze here rather than relying on the windowed
    Silver aggregates (Silver in transaction_stream.py is pre-aggregated by
    window+method, which loses the per-transaction_id granularity this join
    needs). This is a deliberate architecture note worth calling out in
    interviews: not every Silver table is joinable — some are aggregates,
    some are row-level, and you need to know which is which.
    """
    from pyspark.sql.functions import from_json, col as c
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("amount_inr", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("bank_response_code", StringType(), True),
        StructField("response_time_ms", IntegerType(), True),
        StructField("is_international", BooleanType(), True),
        StructField("device_type", StringType(), True),
        StructField("city", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])

    bronze_txn_path = to_spark_path(PROJECT_ROOT / "data" / "processed" / "bronze_transactions")
    raw = spark.read.format("delta").load(bronze_txn_path)
    parsed = (
        raw.select(from_json(c("value"), schema).alias("data"))
        .select("data.*")
        .filter(c("status") == "success")
    )
    return parsed


def get_settlements(spark):
    return spark.read.format("delta").load(SILVER_SETTLEMENT_PATH)


def find_mismatches(txns, settlements):
    """
    Left join transactions -> settlements on transaction_id.
    - Null settlement + txn older than threshold = SETTLEMENT_DELAYED
    - Settlement with no matching transaction_id = ORPHAN_SETTLEMENT
    - Matched but amount differs = AMOUNT_MISMATCH
    """
    joined = txns.alias("t").join(
        settlements.alias("s"),
        on=col("t.transaction_id") == col("s.transaction_id"),
        how="left_outer",
    )

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=SETTLEMENT_DELAY_THRESHOLD_SECONDS)
    cutoff_str = cutoff.isoformat()

    delayed = (
        joined
        .filter(col("s.settlement_id").isNull())
        .filter(col("t.timestamp") < cutoff_str)
        .select(col("t.transaction_id"), col("t.merchant_id"), col("t.amount_inr"))
    )

    matched = joined.filter(col("s.settlement_id").isNotNull())
    amount_mismatch = (
        matched
        .filter(col("t.amount_inr") != col("s.settled_amount_inr"))
        .select(
            col("t.transaction_id"), col("t.merchant_id"),
            col("t.amount_inr").alias("txn_amount"),
            col("s.settled_amount_inr").alias("settled_amount"),
        )
    )

    txn_ids = [r["transaction_id"] for r in txns.select("transaction_id").collect()]
    orphans = settlements.filter(~col("transaction_id").isin(txn_ids)) if txn_ids else settlements

    return delayed, orphans, amount_mismatch


def write_gold(spark, delayed, orphans, amount_mismatch):
    now_str = datetime.now(timezone.utc).isoformat()

    rows = []
    for r in delayed.collect():
        rows.append({
            "mismatch_type": "SETTLEMENT_DELAYED",
            "transaction_id": r["transaction_id"],
            "merchant_id": r["merchant_id"],
            "detail": f"amount_inr={r['amount_inr']}",
            "detected_at": now_str,
        })
    for r in orphans.collect():
        rows.append({
            "mismatch_type": "ORPHAN_SETTLEMENT",
            "transaction_id": r["transaction_id"],
            "merchant_id": r["merchant_id"],
            "detail": f"settlement_id={r['settlement_id']}",
            "detected_at": now_str,
        })
    for r in amount_mismatch.collect():
        rows.append({
            "mismatch_type": "AMOUNT_MISMATCH",
            "transaction_id": r["transaction_id"],
            "merchant_id": r["merchant_id"],
            "detail": f"txn={r['txn_amount']} settled={r['settled_amount']}",
            "detected_at": now_str,
        })

    if not rows:
        print(f"[{now_str}] No mismatches found.")
        return

    df = spark.createDataFrame(rows)
    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(GOLD_MISMATCH_PATH)
    )
    print(f"[{now_str}] Wrote {len(rows)} mismatch records: "
          f"{sum(1 for r in rows if r['mismatch_type']=='SETTLEMENT_DELAYED')} delayed, "
          f"{sum(1 for r in rows if r['mismatch_type']=='ORPHAN_SETTLEMENT')} orphan, "
          f"{sum(1 for r in rows if r['mismatch_type']=='AMOUNT_MISMATCH')} amount mismatch")


def run_once():
    spark = build_spark()
    try:
        txns = get_successful_transactions(spark)
        settlements = get_settlements(spark)
        delayed, orphans, amount_mismatch = find_mismatches(txns, settlements)
        write_gold(spark, delayed, orphans, amount_mismatch)
    finally:
        spark.stop()


if __name__ == "__main__":
    run_once()