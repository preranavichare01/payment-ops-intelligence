# System Design — Payment Ops Intelligence Platform

This document covers the technical design decisions, data flow, scaling
considerations, and honest trade-offs made while building this platform.
It's written the way I'd walk an interviewer through the system.

---

## 1. Problem framing

Payment platforms (UPI switches, PSPs, banks) need real-time visibility
into *why* transactions are failing — not just that they are. A spike in
failures could be: a specific bank's rail being slow, an NPCI-wide outage,
or a bug in the platform's own code. Each of these needs a different
response, and misattributing the cause wastes engineering time during an
incident.

This platform simulates that diagnostic layer: ingest transaction,
settlement, and payout events in real time, detect anomalies, and
*classify* the failure's likely source by correlating multiple streams
against each other.

---

## 2. Data flow

```
Producers (Kafka)
  → transactions, settlements, payouts topics
  → realistic distributions + simulated bank outages / settlement delays

Spark Structured Streaming (per topic)
  → Bronze: raw JSON, append-only, no parsing
  → Silver: schema-validated, watermarked (60s), malformed records → DLQ
  → windowed aggregations for health metrics

Correlation Engine (streaming)
  → joins transaction failure patterns across banks/payment methods
  → classifies: BANK_SIDE_ISSUE / NPCI_SIDE_ISSUE / PLATFORM_SIDE_ISSUE / HEALTHY
  → writes to gold_correlation_events

Multi-Stream Join (streaming)
  → joins transactions ↔ settlements on transaction_id
  → detects SETTLEMENT_DELAYED / ORPHAN_SETTLEMENT / AMOUNT_MISMATCH
  → writes to gold_settlement_mismatches

Gold Mart Layer (dbt views + PySpark materialization)
  → mart_ops_dashboard_metrics       (volume, success rate, latency by method/category)
  → mart_settlement_reconciliation   (delay/discrepancy by merchant)
  → mart_correlation_summary         (latest health classification)

Consumers
  → Streamlit dashboard (live, reads Gold directly)
  → Power BI (CSV export of Gold marts)
  → Groq AI agent (reads Gold via LangChain tools, produces NL report)
  → Great Expectations (validates Gold marts before they're trusted downstream)

Orchestration & Monitoring
  → Airflow: kafka_lag_monitor (every 2 min), streaming_health_monitor (every 5 min)
```

---

## 3. Why watermarking and DLQ matter here

Structured Streaming needs a watermark to know when it's safe to stop
waiting for late data and finalize a windowed aggregation. A 60-second
watermark was chosen because the simulated event generators introduce
realistic but bounded network/processing delay — long enough to absorb
that jitter, short enough to keep dashboards feeling "live."

The DLQ (dead-letter queue) path catches any record that fails schema
parsing rather than silently dropping it or crashing the stream. In this
run, both `dlq_transactions` and `dlq_settlements` show 0 malformed
records — meaning the producer's data contract held for the full run.
That's confirmed via the data quality checks in
`data_quality/gold_mart_checks.py`, not assumed.

---

## 4. Correlation logic — how "whose fault is it" gets decided

The correlation engine looks at failure patterns across payment methods
(`upi_hdfc`, `upi_icici`, `upi_sbi`, etc.) within a time window:

- If failures cluster around **one specific bank's UPI handle** while
  others stay healthy → `BANK_SIDE_ISSUE`
- If failures spike **across all UPI handles simultaneously** → `NPCI_SIDE_ISSUE`
  (the shared rail, not any one bank, is the likely cause)
- If failures don't correlate with any bank pattern but response times or
  error codes point to the platform's own processing → `PLATFORM_SIDE_ISSUE`
- Otherwise → `HEALTHY`

This mirrors how a real ops team would triage: correlate the blast radius
of a failure before assuming which system owns the fix.

---

## 5. Known engineering trade-off: dbt-spark + Delta materialization

**The problem:** dbt-spark 1.10.3, combined with Delta Lake's V2 catalog
integration, fails on `CREATE OR REPLACE TABLE AS SELECT` for any
materialized `table` or `incremental` model, regardless of config. The
error is a `TRUNCATE` capability mismatch — Delta's managed
`spark_catalog` tables in this version combination don't support the
operation dbt-spark's CTAS pattern requires.

**Why I didn't "just fix" the dbt config:** this is a confirmed
version-compatibility bug between dbt-spark 1.10.3 and Delta 3.2.0, not a
misconfiguration on my end — I tried multiple materialization configs
before concluding this was a genuine incompatibility rather than something
solvable by changing `dbt_project.yml`.

**The decision:** split responsibilities cleanly.
- dbt owns transformation *logic* — staging views (`stg_silver_transactions`,
  `stg_silver_settlements`) and intermediate views (`int_bank_health`,
  `int_payment_enriched`) run correctly as dbt views.
- A standalone PySpark script (`batch/dbt_gold/build_gold_marts.py`)
  handles the final Gold materialization step directly via
  `.write.format("delta").mode("overwrite")`.

This is a deliberate architecture choice, documented rather than hidden —
in a real team, I'd raise this as a dbt-spark version bug, but for a
portfolio timeline, working around it with a clear division of
responsibility was the right call.

---

## 6. What production would look like (and why this doesn't build it)

This is a self-directed portfolio project with no cloud budget. The
architecture is designed *as if* it would run on:

- **Google Cloud Storage** for Delta table storage and checkpoints
  (replacing local disk)
- **Databricks** as the lakehouse runtime, with **Unity Catalog** for
  centralized governance, lineage, and access control across the
  Bronze/Silver/Gold layers
- A managed Kafka service (Confluent Cloud or GCP Pub/Sub with a Kafka-
  compatible layer) instead of local Docker Kafka
- Airflow on Cloud Composer instead of local Docker containers

None of this was built because it requires a paid cloud account, and I
made a deliberate choice not to fabricate cloud integration I hadn't
actually implemented. Everything in this repo runs on local
infrastructure and is genuinely reproducible by anyone who clones it — no
cloud credentials required.

---

## 7. Scaling considerations

If this were to actually scale to production transaction volumes:

- **Kafka partitioning**: topics would need to be partitioned by a key
  that distributes load evenly (e.g., `merchant_id` hash) rather than
  the current single-partition local setup, to parallelize consumption.
- **Spark cluster sizing**: `maxOffsetsPerTrigger` (currently 1000) and
  shuffle partitions (currently 4, tuned for a laptop) would need to
  scale with cluster size and actual throughput requirements.
- **Watermark tuning**: 60 seconds works for simulated data; real
  production latency profiles (network jitter, bank response times)
  would need to be measured and the watermark adjusted accordingly —
  too short drops legitimately late data, too long delays finalization.
- **Correlation engine window size**: currently tuned for demo-scale
  data; at real volume, the failure-clustering logic would need
  statistical significance testing (e.g., is this bank's failure rate
  actually anomalous, or within normal variance for its volume?) rather
  than simple thresholding.
- **Gold mart materialization**: the current `overwrite` mode rebuilds
  marts from scratch each run. At scale, this would move to incremental
  merge logic (once the dbt-spark/Delta compatibility issue is resolved
  in a future dbt-spark version, or by using Databricks' native Delta
  support instead of open-source Delta on plain Spark).

---

## 8. Data quality approach

Rather than a full Great Expectations Data Context/Docs setup, quality
gates are intentionally lightweight (`data_quality/gold_mart_checks.py`):

- Not-null checks on key dimension columns
- Value-range sanity checks (no negative volumes, transaction counts)
- Logical consistency checks across related fields (e.g.
  `success_count` can never exceed `total_transactions`)
- DLQ row-count monitoring as a proxy for upstream data contract health

This mirrors how a real team would gate a Gold layer before it reaches
BI tools or the AI agent — catch structural breaks early, without
over-engineering the validation framework for a project at this scale.
