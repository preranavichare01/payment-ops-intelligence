
"""
Exports Gold Delta marts as single clean Parquet files for Power BI Desktop
to connect to directly — no warehouse/cloud needed.
"""
import os, sys
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages io.delta:delta-spark_2.12:3.2.0 "
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
    "pyspark-shell"
)

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = PROJECT_ROOT / "data" / "processed"
EXPORT_DIR = PROJECT_ROOT / "data" / "powerbi_export"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

TABLES = ["mart_ops_dashboard_metrics", "mart_settlement_reconciliation", "mart_correlation_summary"]

def to_spark_path(p: Path) -> str:
    return "file:///" + str(p).replace("\\", "/")

spark = SparkSession.builder.appName("PowerBIExport").config("spark.sql.shuffle.partitions", "4").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

for table in TABLES:
    df = spark.read.format("delta").load(to_spark_path(DATA_ROOT / table))
    pdf = df.toPandas()
    out_path = EXPORT_DIR / f"{table}.csv"
    pdf.to_csv(out_path, index=False)
    print(f"Exported {table}: {len(pdf)} rows -> {out_path}")

spark.stop()
print("Power BI export complete. Point Power BI Desktop at data/powerbi_export/*.parquet")