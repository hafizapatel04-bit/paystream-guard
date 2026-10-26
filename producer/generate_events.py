import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

CATEGORIES = ["GROCERY", "FOOD", "FUEL", "TRAVEL", "UTILITIES", "SHOPPING", "OTHER"]
CATEGORY_WEIGHTS = [30, 25, 10, 8, 10, 12, 5]


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def generate(n_transactions, seed, start, n_payers=500, success_rate=0.95):
    rng = random.Random(seed)
    payers = [f"acc_{i:04d}" for i in range(n_payers)]
    events, event_n, t = [], 0, start

    for txn_n in range(1, n_transactions + 1):
        t += timedelta(seconds=rng.expovariate(1 / 2))  # ~1 txn every 2s
        payer = rng.choice(payers)
        category = rng.choices(CATEGORIES, CATEGORY_WEIGHTS)[0]
        amount = round(min(rng.lognormvariate(6.0, 1.0), 200000), 2)
        final = "SUCCESS" if rng.random() < success_rate else "FAILED"

        for status, delay in (("INITIATED", 0), (final, rng.randint(1, 5))):
            event_n += 1
            event_time = t + timedelta(seconds=delay)
            events.append({
                "event_id": f"evt_{event_n:07d}",
                "transaction_id": f"txn_{txn_n:06d}",
                "event_time": iso(event_time),
                "ingestion_time": iso(event_time + timedelta(seconds=rng.randint(1, 3))),
                "payer_id": payer,
                "amount_inr": amount,
                "payment_status": status,
                "merchant_category": category,
                "payment_method": "UPI",
                "source_system": "simulator",
            })
    return events


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--n-transactions", type=int, default=500)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--start", default="2026-10-26T00:00:00")
    p.add_argument("--out", default="data/events_clean.jsonl")
    a = p.parse_args()

    start = datetime.fromisoformat(a.start).replace(tzinfo=timezone.utc)
    events = generate(a.n_transactions, a.seed, start)

    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    print(f"Wrote {len(events)} events to {out}")
