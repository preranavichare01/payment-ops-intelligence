import os
import time
from pathlib import Path
from dotenv import load_dotenv
from confluent_kafka import Producer

from utils import build_kafka_config
from transaction_generator import generate_transaction, maybe_trigger_bank_failure
from settlement_generator import generate_settlement
from payout_generator import generate_payout, maybe_trigger_city_delay

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / "config" / ".env")

EVENTS_PER_SECOND = int(os.getenv("PRODUCER_EVENTS_PER_SECOND", "50"))
FAILURE_INTERVAL_S = int(os.getenv("FAILURE_INTERVAL_SECONDS", "600"))
FAILURE_DURATION_S = int(os.getenv("FAILURE_DURATION_SECONDS", "180"))

bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
if not bootstrap:
    raise RuntimeError(
        f"KAFKA_BOOTSTRAP_SERVERS not found. Checked: {PROJECT_ROOT / 'config' / '.env'}"
    )

conf = build_kafka_config(bootstrap)
producer = Producer(conf)


def delivery_callback(err, msg):
    if err is not None:
        print(f"[DELIVERY FAILED] {err}")


def run():
    interval = 1.0 / EVENTS_PER_SECOND
    payout_counter = 0

    print(f"Producing {EVENTS_PER_SECOND} txn/sec -> transactions | settlements | payouts (occasional)")

    while True:
        maybe_trigger_bank_failure(EVENTS_PER_SECOND, FAILURE_INTERVAL_S, FAILURE_DURATION_S)

        txn = generate_transaction()
        producer.produce("transactions", key=txn.transaction_id, value=txn.to_json(), callback=delivery_callback)

        if txn.status == "success":
            settlement = generate_settlement(txn.transaction_id, txn.merchant_id, txn.amount_inr)
            producer.produce("settlements", key=settlement.transaction_id, value=settlement.to_json(), callback=delivery_callback)

        payout_counter += 1
        if payout_counter % (EVENTS_PER_SECOND * 5) == 0:
            maybe_trigger_city_delay()
            payout = generate_payout()
            producer.produce("payouts", key=payout.payout_id, value=payout.to_json(), callback=delivery_callback)

        producer.poll(0)
        time.sleep(interval)


if __name__ == "__main__":
    run()