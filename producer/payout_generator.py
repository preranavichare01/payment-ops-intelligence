"""
Generates payout events for drivers/delivery partners across platforms.
City-level delay simulation runs every ~2 hours, mirroring how a real
regional payout batch failure would look (e.g. one city's bank rail is slow).
"""

import random
from datetime import datetime, timedelta, timezone
from utils import PayoutEvent

PLATFORMS = ["uber", "swiggy", "zomato", "zepto", "blinkit"]
RECIPIENT_TYPES = ["driver", "restaurant", "delivery_partner"]
CITIES = ["Mumbai", "Bengaluru", "Delhi", "Pune", "Hyderabad", "Chennai", "Kolkata"]

_city_delay_state = {"city": None, "until": None}


def maybe_trigger_city_delay():
    global _city_delay_state
    now = datetime.now(timezone.utc)
    if _city_delay_state["until"] and now < _city_delay_state["until"]:
        return
    if _city_delay_state["until"] and now >= _city_delay_state["until"]:
        _city_delay_state = {"city": None, "until": None}

    if random.random() < 0.0005:  # roughly every ~2 hours at normal call frequency
        city = random.choice(CITIES)
        _city_delay_state = {"city": city, "until": now + timedelta(hours=1)}
        print(f"[SIMULATED CITY DELAY] {city} payouts delayed until {_city_delay_state['until']}")


def generate_payout() -> PayoutEvent:
    now = datetime.now(timezone.utc)
    normal_delay_hours = random.uniform(2, 4)
    expected_time = now + timedelta(hours=normal_delay_hours)

    city_affected = _city_delay_state["city"] is not None
    extra_delay_hours = random.uniform(3, 8) if city_affected else 0
    actual_delay_hours = normal_delay_hours + extra_delay_hours
    actual_time = now + timedelta(hours=actual_delay_hours)

    status = "delayed" if extra_delay_hours > 0 else "paid"

    return PayoutEvent(
        recipient_id=f"recipient_{random.randint(1, 20000)}",
        recipient_type=random.choice(RECIPIENT_TYPES),
        platform=random.choice(PLATFORMS),
        payout_amount_inr=round(random.uniform(200, 8000), 2),
        payout_status=status,
        expected_payout_time=expected_time.isoformat(),
        actual_payout_time=actual_time.isoformat(),
        delay_minutes=int(actual_delay_hours * 60),
        bank_account_hash=f"hash_{random.randint(100000,999999)}",
    )