import argparse
import json
import time
from pathlib import Path

from confluent_kafka import Producer


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--file", default="data/events_faulty.jsonl")
    p.add_argument("--topic", default="payment-events")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--rate", type=float, default=0, help="events/sec, 0 = as fast as possible")
    p.add_argument("--limit", type=int, default=0, help="send only the first N events")
    a = p.parse_args()

    stats = {"delivered": 0, "failed": 0}

    def on_delivery(err, msg):
        if err is not None:
            stats["failed"] += 1
            print(f"Delivery failed: {err}")
        else:
            stats["delivered"] += 1

    producer = Producer({
        "bootstrap.servers": a.bootstrap,
        "enable.idempotence": True,
        "linger.ms": 20,
    })

    lines = [l for l in Path(a.file).read_text().splitlines() if l.strip()]
    if a.limit:
        lines = lines[:a.limit]

    start = time.time()
    for i, line in enumerate(lines, 1):
        key = None
        try:
            tid = json.loads(line).get("transaction_id")
            key = tid.encode() if tid else None
        except json.JSONDecodeError:
            pass  # malformed JSON still gets sent, with no key

        while True:
            try:
                producer.produce(a.topic, value=line.encode(), key=key, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)

        if a.rate > 0:
            ahead = i / a.rate - (time.time() - start)
            if ahead > 0:
                time.sleep(ahead)

    producer.flush()
    elapsed = time.time() - start
    print(f"Sent {len(lines)} | delivered {stats['delivered']} | failed {stats['failed']} "
          f"| {elapsed:.1f}s | {len(lines) / elapsed:.0f} events/sec")


if __name__ == "__main__":
    main()
