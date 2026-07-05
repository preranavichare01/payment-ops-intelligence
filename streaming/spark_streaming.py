"""
Reads live transaction events from Kafka, parses them into a typed schema,
computes 5-minute sliding windows (1-minute slide) of payment health metrics,
and writes results to Delta Lake Bronze/Silver layers.

Run this AFTER producer.py has pushed data into the 'transactions' topic —
Spark will pick up from the earliest offset by default on first run.
"""

import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, count, sum as spark_sum,
    when, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_PATH = str(PROJECT_ROOT / "data" / "processed" / "bronze_transactions")
SILVER_PATH = str(PROJECT_ROOT / "data" / "processed" / "silver_transactions")
DLQ_PATH = str(PROJECT_ROOT / "data" / "processed" / "dlq_transactions")
CHECKPOINT_ROOT = str(PROJECT_ROOT / "data" / "checkpoints")

KAFKA_BOOTSTRAP = "localhost:9092"

# --- Build Spark session with Delta Lake support ---
builder = (
    SparkSession.builder
    .appName("TransactionStreamProcessor")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "4")  # small for local dev; default 200 is overkill on a laptop
)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# --- Typed schema matching TransactionEvent from producer/utils.py ---
transaction_schema = StructType([
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
    StructField("timestamp", StringType(), True),  # parsed to timestamp type below
])

# --- Read raw stream from Kafka ---
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", "transactions")
    .option("startingOffsets", "earliest")   # replay everything already in the topic
    .option("maxOffsetsPerTrigger", 1000)    # backpressure: cap events per micro-batch
    .load()
)

# --- Bronze: raw events, append-only, no parsing — the "what actually arrived" audit trail ---
bronze_query = (
    raw_stream
    .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value", "timestamp as kafka_timestamp")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze_transactions")
    .start(BRONZE_PATH)
)

# --- Parse JSON payload into typed columns ---
parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_timestamp")
    .select(
        col("json_value"),
        from_json(col("json_value"), transaction_schema).alias("data"),
        col("kafka_timestamp")
    )
)

# --- Schema validation: rows where JSON parsing failed (from_json returns null on bad JSON) ---
valid_rows = parsed_stream.filter(col("data").isNotNull()).select("data.*", "kafka_timestamp")
dlq_rows = parsed_stream.filter(col("data").isNull()).select("json_value", "kafka_timestamp")

# --- DLQ: malformed events, so nothing silently disappears ---
dlq_query = (
    dlq_rows
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/dlq_transactions")
    .start(DLQ_PATH)
)

# --- Cast timestamp string to real timestamp type for windowing ---
enriched = valid_rows.withColumn("event_time", col("timestamp").cast("timestamp"))

# --- Watermark: tolerate events arriving up to 60 seconds late ---
# Why 60 seconds specifically: UPI callbacks (bank confirming success/failure) can legitimately
# arrive after the transaction event itself, due to NPCI round-trip + bank processing latency.
# Too short a watermark -> we drop legitimately late UPI confirmations, undercounting success rate.
# Too long -> state is held in memory longer, increasing memory pressure per window.
# 60s is a reasonable middle ground based on typical UPI callback latency profiles.
watermarked = enriched.withWatermark("event_time", "60 seconds")

# --- Windowed aggregation: 5-minute window, 1-minute slide ---
# Why sliding not tumbling: ops teams watching a dashboard want a metric that updates every
# minute, not one that resets and stays empty for 4 minutes before showing anything (tumbling).
# Sliding windows give a moving-average feel that's far more usable for live monitoring.
windowed_metrics = (
    watermarked
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute"),
        col("payment_method")
    )
    .agg(
        count("*").alias("total_transactions"),
        spark_sum(when(col("status") == "success", 1).otherwise(0)).alias("success_count"),
        spark_sum(when(col("status") == "timeout", 1).otherwise(0)).alias("timeout_count"),
        avg("response_time_ms").alias("avg_response_time_ms"),
    )
    .withColumn("success_rate_pct", (col("success_count") / col("total_transactions")) * 100)
    .withColumn("timeout_rate_pct", (col("timeout_count") / col("total_transactions")) * 100)
)

# --- Silver: parsed, enriched, windowed metrics ---
silver_query = (
    windowed_metrics
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver_transactions")
    .start(SILVER_PATH)
)

print("Streaming started. Bronze / Silver / DLQ writers are running.")
print(f"Bronze path: {BRONZE_PATH}")
print(f"Silver path: {SILVER_PATH}")
print(f"DLQ path:    {DLQ_PATH}")
print("Press Ctrl+C to stop.")

spark.streams.awaitAnyTermination()