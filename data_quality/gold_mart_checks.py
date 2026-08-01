"""
Lightweight data quality checks on Gold marts using Great Expectations.

Design note: kept intentionally small — a handful of high-value checks per
mart (not-null on key columns, logical consistency between related fields,
value-range sanity checks) rather than a full GE Data Context / Docs setup.
This mirrors how a real team would gate a Gold layer: catch structural
breaks (nulls, impossible values) before they reach BI tools or the AI
ops agent, without over-engineering the validation framework itself.

Run manually after building Gold marts, or wire into Airflow as a
downstream task on the existing DAGs.
"""

import os
import sys
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages io.delta:delta-spark_2.12:3.2.0 "
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
    "pyspark-shell"
)

import great_expectations as gx
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = PROJECT_ROOT / "data" / "processed"


def to_spark_path(p: Path) -> str:
    return "file:///" + str(p).replace("\\", "/")


def load_as_pandas(spark, table_name: str):
    return spark.read.format("delta").load(to_spark_path(DATA_ROOT / table_name)).toPandas()


def run_checks_on_dataframe(context, source_name: str, asset_name: str, df, checks: list):
    """
    checks: list of (description, expectation_object) tuples.
    Returns list of (description, success: bool, details: str).
    """
    data_source = context.data_sources.add_pandas(source_name)
    data_asset = data_source.add_dataframe_asset(name=asset_name)
    batch_definition = data_asset.add_batch_definition_whole_dataframe("batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    results = []
    for description, expectation in checks:
        result = batch.validate(expectation)
        results.append((description, result.success, result.result))
    return results


def print_results(mart_name: str, results):
    print(f"\n{'=' * 60}")
    print(f"  {mart_name}")
    print(f"{'=' * 60}")
    passed = sum(1 for _, success, _ in results if success)
    for description, success, detail in results:
        status = "PASS" if success else "FAIL"
        print(f"  [{status}] {description}")
        if not success:
            print(f"         -> {detail}")
    print(f"  {passed}/{len(results)} checks passed")


def run():
    spark = (
        SparkSession.builder
        .appName("GoldMartDataQualityChecks")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    context = gx.get_context()

    # ---------------- mart_ops_dashboard_metrics ----------------
    ops_df = load_as_pandas(spark, "mart_ops_dashboard_metrics")
    ops_checks = [
        (
            "payment_method has no nulls",
            gx.expectations.ExpectColumnValuesToNotBeNull(column="payment_method"),
        ),
        (
            "merchant_category has no nulls",
            gx.expectations.ExpectColumnValuesToNotBeNull(column="merchant_category"),
        ),
        (
            "total_transactions is non-negative",
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="total_transactions", min_value=0, max_value=None
            ),
        ),
        (
            "total_volume_inr is non-negative",
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="total_volume_inr", min_value=0, max_value=None
            ),
        ),
        (
            "avg_response_time_ms is within a sane range (0-60000ms)",
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="avg_response_time_ms", min_value=0, max_value=60000
            ),
        ),
    ]
    ops_results = run_checks_on_dataframe(context, "ops_src", "ops_asset", ops_df, ops_checks)
    print_results("mart_ops_dashboard_metrics", ops_results)

    # success_count <= total_transactions — logical consistency, not a
    # single-column GE expectation, so checked directly in pandas.
    bad_rows = ops_df[ops_df["success_count"] > ops_df["total_transactions"]]
    consistency_ok = len(bad_rows) == 0
    status = "PASS" if consistency_ok else "FAIL"
    print(f"  [{status}] success_count never exceeds total_transactions")
    if not consistency_ok:
        print(f"         -> {len(bad_rows)} row(s) violate this")

    # ---------------- mart_settlement_reconciliation ----------------
    recon_df = load_as_pandas(spark, "mart_settlement_reconciliation")
    recon_checks = [
        (
            "merchant_id has no nulls",
            gx.expectations.ExpectColumnValuesToNotBeNull(column="merchant_id"),
        ),
        (
            "total_settled_transactions is non-negative",
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="total_settled_transactions", min_value=0, max_value=None
            ),
        ),
        (
            "delayed_count is non-negative",
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="delayed_count", min_value=0, max_value=None
            ),
        ),
    ]
    recon_results = run_checks_on_dataframe(context, "recon_src", "recon_asset", recon_df, recon_checks)
    print_results("mart_settlement_reconciliation", recon_results)

    # delayed_count <= total_settled_transactions — logical consistency
    bad_rows = recon_df[recon_df["delayed_count"] > recon_df["total_settled_transactions"]]
    consistency_ok = len(bad_rows) == 0
    status = "PASS" if consistency_ok else "FAIL"
    print(f"  [{status}] delayed_count never exceeds total_settled_transactions")
    if not consistency_ok:
        print(f"         -> {len(bad_rows)} row(s) violate this")

    # ---------------- DLQ health check ----------------
    print(f"\n{'=' * 60}")
    print("  DLQ Health Check")
    print(f"{'=' * 60}")
    for dlq_table in ["dlq_transactions", "dlq_settlements"]:
        try:
            count = spark.read.format("delta").load(to_spark_path(DATA_ROOT / dlq_table)).count()
            status = "PASS" if count == 0 else "ATTENTION"
            print(f"  [{status}] {dlq_table}: {count} malformed record(s)")
        except Exception as e:
            print(f"  [SKIP] {dlq_table}: not found ({e})")

    spark.stop()


if __name__ == "__main__":
    run()