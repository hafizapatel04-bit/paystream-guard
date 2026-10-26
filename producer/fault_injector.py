import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

FMT = "%Y-%m-%dT%H:%M:%SZ"


def parse(s):
    return datetime.strptime(s, FMT).replace(tzinfo=timezone.utc)


def iso(dt):
    return dt.strftime(FMT)


def inject(events, seed, dup_rate=0.02, bad_amount_rate=0.01, missing_id_rate=0.01,
           late_rate=0.03, conflict_rate=0.01, spike_txns=40):
    rng = random.Random(seed)
    n = len(events)
    items = [[float(i), dict(e)] for i, e in enumerate(events)]  # [stream position, event]
    idx = list(range(n))
    rng.shuffle(idx)
    cursor = 0
    next_id = 9000001
    manifest = {"seed": seed, "clean_events": n}

    def take(k):
        nonlocal cursor
        chosen = idx[cursor:cursor + k]
        cursor += k
        return chosen

    # 1. invalid amounts -> quarantine
    chosen = take(int(n * bad_amount_rate))
    for i in chosen:
        items[i][1]["amount_inr"] = -abs(items[i][1]["amount_inr"])
    manifest["invalid_amount"] = [events[i]["event_id"] for i in chosen]

    # 2. missing transaction_id -> quarantine
    chosen = take(int(n * missing_id_rate))
    for i in chosen:
        items[i][1].pop("transaction_id", None)
    manifest["missing_transaction_id"] = [events[i]["event_id"] for i in chosen]

    # 3. late events: ingested 10-20 min after event_time and delivered later in the stream
    chosen = take(int(n * late_rate))
    for i in chosen:
        e = items[i][1]
        e["ingestion_time"] = iso(parse(e["event_time"]) + timedelta(minutes=rng.randint(10, 20)))
        items[i][0] += rng.randint(600, 1200)
    manifest["late_events"] = [events[i]["event_id"] for i in chosen]

    # 4. duplicates: same event_id sent again a bit later
    chosen = take(int(n * dup_rate))
    for i in chosen:
        items.append([items[i][0] + rng.uniform(5, 200), dict(items[i][1])])
    manifest["duplicates"] = [events[i]["event_id"] for i in chosen]

    # 5. status conflicts: a SUCCESS transaction also receives a FAILED event
    candidates = [i for i in idx[cursor:] if events[i]["payment_status"] == "SUCCESS"]
    chosen = candidates[:int(n * conflict_rate)]
    conflict_ids = []
    for i in chosen:
        e = dict(items[i][1])
        e["event_id"] = f"evt_{next_id:07d}"
        next_id += 1
        e["payment_status"] = "FAILED"
        e["event_time"] = iso(parse(e["event_time"]) + timedelta(seconds=2))
        e["ingestion_time"] = iso(parse(e["event_time"]) + timedelta(seconds=1))
        items.append([items[i][0] + 3, e])
        conflict_ids.append(e["transaction_id"])
    manifest["status_conflict_transactions"] = conflict_ids

    # 6. spike: one synthetic payer makes many payments in about a minute
    base = parse(events[n // 2]["event_time"])
    spike_events = 0
    for j in range(spike_txns):
        amount = round(rng.uniform(100, 5000), 2)
        for status, delay in (("INITIATED", 0), ("SUCCESS", 1)):
            et = base + timedelta(seconds=j * 1.5 + delay)
            items.append([n / 2 + j * 2 + delay * 0.5, {
                "event_id": f"evt_{next_id:07d}",
                "transaction_id": f"txn_S{j:04d}",
                "event_time": iso(et),
                "ingestion_time": iso(et + timedelta(seconds=rng.randint(1, 3))),
                "payer_id": "acc_SPIKE",
                "amount_inr": amount,
                "payment_status": status,
                "merchant_category": "SHOPPING",
                "payment_method": "UPI",
                "source_system": "simulator",
            }])
            next_id += 1
            spike_events += 1
    manifest["spike"] = {"payer_id": "acc_SPIKE", "transactions": spike_txns, "events": spike_events}

    items.sort(key=lambda x: x[0])
    out_events = [x[1] for x in items]

    manifest["total_output_events"] = len(out_events)
    manifest["expected"] = {
        "quarantined": len(manifest["invalid_amount"]) + len(manifest["missing_transaction_id"]),
        "duplicates_to_drop": len(manifest["duplicates"]),
        "late_events": len(manifest["late_events"]),
        "status_conflicts": len(conflict_ids),
        "spike_payer_events": spike_events,
    }
    return out_events, manifest


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="inp", default="data/events_clean.jsonl")
    p.add_argument("--out", default="data/events_faulty.jsonl")
    p.add_argument("--manifest", default="data/fault_manifest.json")
    p.add_argument("--seed", type=int, default=7)
    a = p.parse_args()

    events = [json.loads(line) for line in Path(a.inp).read_text().splitlines()]
    out_events, manifest = inject(events, a.seed)

    Path(a.out).write_text("\n".join(json.dumps(e) for e in out_events) + "\n")
    Path(a.manifest).write_text(json.dumps(manifest, indent=2))
    print(f"Clean: {manifest['clean_events']} -> Faulty: {manifest['total_output_events']}")
    for k, v in manifest["expected"].items():
        print(f"  {k}: {v}")
