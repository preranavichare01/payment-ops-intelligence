"""
Generates a settlement event for a given successful transaction.
Called by producer.py after it sees a 'success' transaction event.
"""

import random
from datetime import datetime, timedelta, timezone
from utils import SettlementEvent


def generate_settlement(transaction_id: str, merchant_id: str, amount_inr: float) -> SettlementEvent:
    now = datetime.now(timezone.utc)
    expected_delay = random.randint(30, 120)  # seconds
    expected_time = now + timedelta(seconds=expected_delay)

    is_failed = random.random() < 0.02
    is_delayed = (not is_failed) and random.random() < 0.03

    if is_failed:
        status = "failed"
        actual_delay = expected_delay
        settled_amount = 0.0
    elif is_delayed:
        status = "settled"
        actual_delay = expected_delay + random.randint(300, 1800)  # blown past expected window
        settled_amount = amount_inr
    else:
        status = "settled"
        actual_delay = expected_delay
        settled_amount = amount_inr

    actual_time = now + timedelta(seconds=actual_delay)

    return SettlementEvent(
        transaction_id=transaction_id,
        merchant_id=merchant_id,
        settled_amount_inr=settled_amount,
        settlement_status=status,
        bank_reference_number=f"REF{random.randint(100000, 999999)}",
        expected_settlement_time=expected_time.isoformat(),
        actual_settlement_time=actual_time.isoformat(),
        delay_seconds=actual_delay,
    )