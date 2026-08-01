"""
Consumes the 'settlements' Kafka topic, parses events, writes Bronze/Silver/DLQ
Delta tables — mirrors transaction_stream.py's pattern for consistency.
"""

import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def to_spark_path(p: Path) -> str:
    return "file:///" + str(p).replace("\\", "/")


BRONZE_PATH = to_spark_path(PROJECT_ROOT / "data" / "processed" / "bronze_settlements")
SILVER_PATH = to_spark_path(PROJECT_ROOT / "data" / "processed" / "silver_settlements")
DLQ_PATH = to_spark_path(PROJECT_ROOT / "data" / "processed" / "dlq_settlements")
CHECKPOINT_ROOT = to_spark_path(PROJECT_ROOT / "data" / "checkpoints")

KAFKA_BOOTSTRAP = "localhost:9092"

builder = (
    SparkSession.builder
    .appName("SettlementStreamProcessor")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "4")
)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

settlement_schema = StructType([
    StructField("settlement_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("settled_amount_inr", DoubleType(), True),
    StructField("settlement_status", StringType(), True),
    StructField("bank_reference_number", StringType(), True),
    StructField("settlement_timestamp", StringType(), True),
    StructField("expected_settlement_time", StringType(), True),
    StructField("actual_settlement_time", StringType(), True),
    StructField("delay_seconds", IntegerType(), True),
])

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", "settlements")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 1000)
    .load()
)

bronze_query = (
    raw_stream
    .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value", "timestamp as kafka_timestamp")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze_settlements")
    .start(BRONZE_PATH)
)

parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_timestamp")
    .select(
        col("json_value"),
        from_json(col("json_value"), settlement_schema).alias("data"),
        col("kafka_timestamp")
    )
)

valid_rows = parsed_stream.filter(col("data").isNotNull()).select("data.*", "kafka_timestamp")
dlq_rows = parsed_stream.filter(col("data").isNull()).select("json_value", "kafka_timestamp")

dlq_query = (
    dlq_rows
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/dlq_settlements")
    .start(DLQ_PATH)
)

# Settlement rows go straight to Silver as parsed events (no windowed aggregation
# needed here — multi_stream_join.py does row-level joins against transactions,
# not aggregate metrics like transaction_stream.py's health windows).
enriched = valid_rows.withColumn("event_time", col("settlement_timestamp").cast("timestamp"))
watermarked = enriched.withWatermark("event_time", "60 seconds")

silver_query = (
    watermarked
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver_settlements")
    .start(SILVER_PATH)
)

print("Settlement streaming started. Bronze / Silver / DLQ writers running.")
print(f"Silver path: {SILVER_PATH}")
print("Press Ctrl+C to stop.")

spark.streams.awaitAnyTermination()