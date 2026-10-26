import argparse
import json
import time
from collections import Counter

from confluent_kafka import Consumer


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--topic", default="payment-events")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--expected", type=int, default=0)
    p.add_argument("--idle-timeout", type=float, default=10)
    a = p.parse_args()

    c = Consumer({
        "bootstrap.servers": a.bootstrap,
        "group.id": f"verify-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    c.subscribe([a.topic])

    total, bad = 0, 0
    ids, partitions = Counter(), Counter()
    last = time.time()
    while time.time() - last < a.idle_timeout:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        last = time.time()
        total += 1
        partitions[msg.partition()] += 1
        try:
            ids[json.loads(msg.value()).get("event_id")] += 1
        except (json.JSONDecodeError, AttributeError):
            bad += 1
    c.close()

    dupes = sum(v - 1 for v in ids.values() if v > 1)
    print(f"Consumed {total} | unparseable {bad} | distinct event_ids {len(ids)} | repeated copies {dupes}")
    print(f"Per partition: {dict(sorted(partitions.items()))}")
    if a.expected:
        print("PASS" if total == a.expected else f"FAIL: expected {a.expected}, got {total}")


if __name__ == "__main__":
    main()
