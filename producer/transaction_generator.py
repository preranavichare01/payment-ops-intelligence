"""
Pure generation logic for transaction events — no Kafka, no I/O.
Kept separate from producer.py so this can be unit tested in isolation
(see tests/test_producer.py) without needing a live Kafka broker.
"""

import random
from datetime import datetime, timedelta, timezone
from utils import TransactionEvent

BANKS_UPI = ["upi_hdfc", "upi_axis", "upi_sbi", "upi_icici"]
WALLETS = ["paytm_wallet", "gpay", "phonepe"]
CARDS = ["card_visa", "card_mastercard"]

MERCHANT_CATEGORIES = ["food_delivery", "quick_commerce", "mobility", "ecommerce", "entertainment"]
CITIES = ["Mumbai", "Bengaluru", "Delhi", "Pune", "Hyderabad", "Chennai", "Kolkata"]
DEVICE_TYPES = ["android", "ios", "web"]

BASE_SUCCESS_RATE = {
    **{b: random.uniform(0.94, 0.97) for b in BANKS_UPI},
    **{w: 0.98 for w in WALLETS},
    **{c: 0.96 for c in CARDS},
}

# module-level state for bank failure simulation
_active_failure = {"bank": None, "until": None}


def maybe_trigger_bank_failure(events_per_second: int, failure_interval_s: int, failure_duration_s: int):
    global _active_failure
    now = datetime.now(timezone.utc)

    if _active_failure["until"] and now < _active_failure["until"]:
        return
    if _active_failure["until"] and now >= _active_failure["until"]:
        _active_failure = {"bank": None, "until": None}

    trigger_prob = 1 / (failure_interval_s * events_per_second)
    if random.random() < trigger_prob:
        bank = random.choice(BANKS_UPI)
        _active_failure = {
            "bank": bank,
            "until": now + timedelta(seconds=failure_duration_s),
        }
        print(f"[SIMULATED OUTAGE] {bank} degraded until {_active_failure['until']}")


def _get_success_rate(method: str) -> float:
    if _active_failure["bank"] == method:
        return random.uniform(0.60, 0.70)
    return BASE_SUCCESS_RATE.get(method, 0.95)


def _pick_payment_method() -> str:
    r = random.random()
    if r < 0.60:
        return random.choice(BANKS_UPI)
    elif r < 0.80:
        return random.choice(WALLETS)
    return random.choice(CARDS)


def generate_transaction() -> TransactionEvent:
    method = _pick_payment_method()
    success_rate = _get_success_rate(method)
    roll = random.random()
    is_degraded = _active_failure["bank"] == method

    if roll < success_rate:
        status = "success"
        response_time = random.randint(200, 1500)
    elif roll < success_rate + (0.02 if is_degraded else 0.005):
        status = "timeout"
        response_time = random.randint(5000, 15000)
    else:
        status = "failed"
        response_time = random.randint(300, 3000)

    return TransactionEvent(
        user_id=f"user_{random.randint(1, 50000)}",
        merchant_id=f"merchant_{random.randint(1, 2000)}",
        merchant_category=random.choice(MERCHANT_CATEGORIES),
        payment_method=method,
        amount_inr=round(random.uniform(50, 5000), 2),
        status=status,
        bank_response_code="00" if status == "success" else random.choice(["51", "91", "96", "TO"]),
        response_time_ms=response_time,
        is_international=random.random() < 0.01,
        device_type=random.choice(DEVICE_TYPES),
        city=random.choice(CITIES),
    )