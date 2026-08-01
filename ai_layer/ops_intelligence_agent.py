"""
LangChain agent that reads Gold Delta tables and produces a natural-language
ops intelligence report — payment ecosystem health score, active anomalies,
correlation findings, and recommended actions.

Design note: tools are read-only Spark SQL queries against Gold tables.
The agent never writes/mutates data — it's an analysis layer on top of
what the pipeline already computed, which keeps the LLM out of the critical
path for actual data correctness (it summarizes, it doesn't calculate).
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timezone

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages io.delta:delta-spark_2.12:3.2.0 "
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
    "pyspark-shell"
)

from pyspark.sql import SparkSession
from langchain.agents import Tool, AgentExecutor, create_tool_calling_agent
from langchain_groq import ChatGroq
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / "config" / ".env")


def to_spark_path(p: Path) -> str:
    return "file:///" + str(p).replace("\\", "/")


spark = (
    SparkSession.builder
    .appName("OpsIntelligenceAgent")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

DATA_ROOT = PROJECT_ROOT / "data" / "processed"


def query_gold_metrics(_input: str = "") -> str:
    """Returns overall payment ecosystem metrics from mart_ops_dashboard_metrics."""
    df = spark.read.format("delta").load(to_spark_path(DATA_ROOT / "mart_ops_dashboard_metrics"))
    row = df.agg({"total_transactions": "sum", "success_count": "sum", "total_volume_inr": "sum"}).collect()[0]
    total = row["sum(total_transactions)"] or 0
    success = row["sum(success_count)"] or 0
    volume = row["sum(total_volume_inr)"] or 0
    rate = (success / total * 100) if total else 0
    return f"Total transactions: {total}, success rate: {rate:.2f}%, total volume: ₹{volume:,.0f}"


def get_correlation_events(_input: str = "") -> str:
    """Returns the most recent cross-bank correlation classifications."""
    df = spark.read.format("delta").load(to_spark_path(DATA_ROOT / "mart_correlation_summary"))
    rows = df.orderBy(df.detected_at.desc()).limit(5).collect()
    if not rows:
        return "No correlation events recorded."
    return "\n".join(
        f"{r['detected_at']}: {r['classification']} (banks: {r['affected_banks'] or 'none'})"
        for r in rows
    )


def get_settlement_mismatches(_input: str = "") -> str:
    """Returns settlement reconciliation health summary."""
    df = spark.read.format("delta").load(to_spark_path(DATA_ROOT / "mart_settlement_reconciliation"))
    row = df.agg({"total_settled_transactions": "sum", "delayed_count": "sum", "amount_discrepancy": "sum"}).collect()[0]
    total = row["sum(total_settled_transactions)"] or 0
    delayed = row["sum(delayed_count)"] or 0
    discrepancy = row["sum(amount_discrepancy)"] or 0
    delay_rate = (delayed / total * 100) if total else 0
    return f"Total settled: {total}, delayed: {delayed} ({delay_rate:.1f}%), amount discrepancy: ₹{discrepancy:,.0f}"


tools = [
    Tool(name="query_gold_metrics", func=query_gold_metrics,
         description="Get overall transaction volume, count, and success rate from the ops dashboard."),
    Tool(name="get_correlation_events", func=get_correlation_events,
         description="Get the most recent cross-bank failure correlation classifications (NPCI-wide, bank-side, platform-side, or healthy)."),
    Tool(name="get_settlement_mismatches", func=get_settlement_mismatches,
         description="Get settlement reconciliation health: delayed settlements and amount discrepancies."),
]


llm = ChatGroq(model="llama-3.3-70b-versatile", temperature=0)

prompt = ChatPromptTemplate.from_messages([
    ("system",
     "You are a payment operations intelligence analyst. Use the available tools to "
     "gather current system state, then produce a structured report with these sections: "
     "1) Payment Ecosystem Health Score (0-100, your own judgment based on success rate, "
     "delays, and correlation events) 2) Active Anomalies with severity 3) Cross-Bank "
     "Correlation Findings 4) Settlement Health 5) Recommended Immediate Actions. "
     "Be concise and concrete — cite actual numbers from the tools, don't speculate."),
    ("human", "{input}"),
    MessagesPlaceholder(variable_name="agent_scratchpad"),
])

agent = create_tool_calling_agent(llm, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)


def generate_report() -> str:
    result = agent_executor.invoke({
        "input": "Generate the current payment operations intelligence report."
    })
    return result["output"]


def save_report(report_text: str):
    from pyspark.sql import Row
    now = datetime.now(timezone.utc).isoformat()
    df = spark.createDataFrame([Row(report=report_text, generated_at=now)])
    path = to_spark_path(DATA_ROOT / "ops_intelligence_reports")
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
    print(f"Report saved at {now}")


if __name__ == "__main__":
    print("Generating ops intelligence report...")
    report = generate_report()
    print("\n" + "=" * 60)
    print(report)
    print("=" * 60 + "\n")
    save_report(report)
    spark.stop()