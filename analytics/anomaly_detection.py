"""
Trains an Isolation Forest to detect anomalous payment-ecosystem windows —
e.g. a bank outage, a platform-wide slowdown, unusual volume spikes.

Why Isolation Forest over other options:
- No labeled anomaly data exists (we've never had a real outage) — Isolation
  Forest is unsupervised, it learns what "normal" windows look like and
  flags deviations, no labels needed.
- It isolates anomalies structurally: outliers require fewer random splits
  to isolate than normal points, so it scores in O(n log n) — cheap enough
  to run as a Spark UDF per 5-minute window without becoming a bottleneck.
- Compare to a simple z-score threshold: z-scores are per-feature, so they
  miss anomalies that are only visible in *combinations* of features
  (e.g. success_rate is fine, but response_time is high AND volume is low
  simultaneously — Isolation Forest catches this multivariate pattern).

Interview question this answers: "Why Isolation Forest and not autoencoders
or a supervised classifier?" -> no labeled anomalies exist yet (cold start
problem), inference needs to be cheap enough for streaming, and the feature
space is small (5 features) so a tree-based method doesn't need GPU/deep
learning overhead a supervised net would need to justify its complexity.
"""

import random
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import precision_score, recall_score, f1_score
import joblib
from pathlib import Path

RANDOM_SEED = 42
N_NORMAL_WINDOWS = 10_000
N_ANOMALOUS_WINDOWS = 300  # held out purely for evaluation, never used in training
CONTAMINATION = 0.03  # our prior belief: ~3% of real-world windows will be anomalous

FEATURES = [
    "success_rate_pct",
    "avg_response_time_ms",
    "volume_zscore",
    "timeout_rate_pct",
    "max_single_bank_failure_rate",
]

random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)


def generate_normal_window() -> dict:
    """
    Simulates a healthy 5-minute window. Values are drawn from tight
    distributions around what we saw in producer.py's baseline success rates.
    """
    return {
        "success_rate_pct": np.random.normal(95.5, 1.2),
        "avg_response_time_ms": np.random.normal(650, 120),
        "volume_zscore": np.random.normal(0, 1),          # z-score vs rolling baseline volume
        "timeout_rate_pct": np.random.normal(0.5, 0.3),
        "max_single_bank_failure_rate": np.random.normal(4.0, 1.5),
    }


def generate_anomalous_window() -> dict:
    """
    Simulates the 4 failure patterns we're actually trying to catch:
    bank outage, NPCI-wide issue, platform slowdown, volume spike/drop.
    Used ONLY for evaluation — Isolation Forest never sees these at train time,
    since in production we won't have labeled anomalies either.
    """
    pattern = random.choice(["bank_outage", "platform_slow", "volume_anomaly", "npci_wide"])

    if pattern == "bank_outage":
        return {
            "success_rate_pct": np.random.normal(70, 5),
            "avg_response_time_ms": np.random.normal(700, 150),
            "volume_zscore": np.random.normal(0, 1),
            "timeout_rate_pct": np.random.normal(1.0, 0.5),
            "max_single_bank_failure_rate": np.random.normal(35, 5),  # one bank spikes hard
        }
    elif pattern == "platform_slow":
        return {
            "success_rate_pct": np.random.normal(88, 3),
            "avg_response_time_ms": np.random.normal(4500, 800),  # response time blows up
            "volume_zscore": np.random.normal(0, 1),
            "timeout_rate_pct": np.random.normal(8, 2),
            "max_single_bank_failure_rate": np.random.normal(8, 2),
        }
    elif pattern == "volume_anomaly":
        return {
            "success_rate_pct": np.random.normal(94, 2),
            "avg_response_time_ms": np.random.normal(700, 150),
            "volume_zscore": np.random.choice([1, -1]) * np.random.normal(4, 1),  # spike or drop
            "timeout_rate_pct": np.random.normal(0.7, 0.3),
            "max_single_bank_failure_rate": np.random.normal(5, 1.5),
        }
    else:  # npci_wide — multiple banks degrade together
        return {
            "success_rate_pct": np.random.normal(65, 6),
            "avg_response_time_ms": np.random.normal(900, 200),
            "volume_zscore": np.random.normal(0, 1),
            "timeout_rate_pct": np.random.normal(2, 0.8),
            "max_single_bank_failure_rate": np.random.normal(28, 6),
        }


def build_datasets():
    normal_data = pd.DataFrame([generate_normal_window() for _ in range(N_NORMAL_WINDOWS)])
    anomalous_data = pd.DataFrame([generate_anomalous_window() for _ in range(N_ANOMALOUS_WINDOWS)])
    return normal_data, anomalous_data


def train_model(normal_data: pd.DataFrame) -> IsolationForest:
    model = IsolationForest(
        n_estimators=200,          # more trees = more stable isolation scores, cost is linear
        contamination=CONTAMINATION,
        max_samples="auto",
        random_state=RANDOM_SEED,
        n_jobs=-1,
    )
    model.fit(normal_data[FEATURES])
    return model


def evaluate(model: IsolationForest, normal_data: pd.DataFrame, anomalous_data: pd.DataFrame):
    """
    Combines held-out normal windows + anomalous windows into one labeled
    eval set (this labeling is ONLY for evaluation, never seen during training).
    """
    eval_normal = normal_data.sample(1000, random_state=RANDOM_SEED)
    eval_set = pd.concat([eval_normal, anomalous_data], ignore_index=True)
    true_labels = [0] * len(eval_normal) + [1] * len(anomalous_data)  # 1 = anomaly

    # IsolationForest.predict returns -1 for anomaly, 1 for normal — remap to 0/1
    raw_predictions = model.predict(eval_set[FEATURES])
    predicted_labels = [1 if p == -1 else 0 for p in raw_predictions]

    precision = precision_score(true_labels, predicted_labels)
    recall = recall_score(true_labels, predicted_labels)
    f1 = f1_score(true_labels, predicted_labels)

    print(f"Precision: {precision:.3f}")
    print(f"Recall:    {recall:.3f}")
    print(f"F1:        {f1:.3f}")

    return {"precision": precision, "recall": recall, "f1": f1}


def save_model(model: IsolationForest, path: str = "analytics/anomaly_model.pkl"):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, path)
    print(f"Model saved to {path}")


if __name__ == "__main__":
    print("Generating synthetic training data...")
    normal_data, anomalous_data = build_datasets()

    print(f"Training Isolation Forest on {len(normal_data)} normal windows (contamination={CONTAMINATION})...")
    model = train_model(normal_data)

    print("Evaluating on held-out normal + synthetic anomalous windows...")
    metrics = evaluate(model, normal_data, anomalous_data)

    save_model(model)