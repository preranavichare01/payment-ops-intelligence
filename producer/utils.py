"""
Shared dataclasses + serialization helpers for all event generators.

Design decision: dataclasses over raw dicts — schema enforced at construction,
not silently drifting. asdict() gives clean JSON for Kafka. This file is the
single source of truth every generator and every consumer imports from.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
import uuid
import json


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class TransactionEvent:
    transaction_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str = ""
    merchant_id: str = ""
    merchant_category: str = ""   # food_delivery | quick_commerce | mobility | ecommerce | entertainment
    payment_method: str = ""      # upi_hdfc | upi_axis | upi_sbi | upi_icici | paytm_wallet | gpay | phonepe | card_visa | card_mastercard
    amount_inr: float = 0.0
    currency: str = "INR"
    status: str = "initiated"     # initiated | processing | success | failed | timeout
    bank_response_code: str = ""
    response_time_ms: int = 0
    is_international: bool = False
    device_type: str = "android"  # android | ios | web
    city: str = ""
    timestamp: str = field(default_factory=now_utc_iso)

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class SettlementEvent:
    settlement_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    transaction_id: str = ""
    merchant_id: str = ""
    settled_amount_inr: float = 0.0
    settlement_status: str = "pending"  # pending | processing | settled | failed | reversed
    bank_reference_number: str = ""
    settlement_timestamp: str = field(default_factory=now_utc_iso)
    expected_settlement_time: str = ""
    actual_settlement_time: str = ""
    delay_seconds: int = 0

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class PayoutEvent:
    payout_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    recipient_id: str = ""
    recipient_type: str = "driver"    # driver | restaurant | delivery_partner
    platform: str = ""                # uber | swiggy | zomato | zepto | blinkit
    payout_amount_inr: float = 0.0
    payout_status: str = "scheduled"  # scheduled | processing | paid | delayed | failed
    expected_payout_time: str = ""
    actual_payout_time: str = ""
    delay_minutes: int = 0
    bank_account_hash: str = ""

    def to_json(self) -> str:
        return json.dumps(asdict(self))


def build_kafka_config(bootstrap_servers: str) -> dict:
    """
    Centralized producer config so every generator/producer uses identical
    exactly-once-relevant settings — no drift between transaction/settlement/payout producers.
    """
    return {
        "bootstrap.servers": bootstrap_servers,
        "acks": "all",                # wait for all in-sync replicas — no silent loss on leader crash
        "retries": 5,                 # retry transient failures instead of dropping events
        "enable.idempotence": True,   # dedupes retried sends via producer ID + sequence number
        "max.in.flight.requests.per.connection": 5,
    }