from datetime import datetime, timezone

from generate_events import generate

START = datetime(2026, 10, 26, tzinfo=timezone.utc)


def test_same_seed_gives_identical_events():
    assert generate(200, 42, START) == generate(200, 42, START)


def test_different_seed_gives_different_events():
    assert generate(200, 1, START) != generate(200, 2, START)


def test_every_transaction_has_initiated_then_one_final_event():
    events = generate(300, 42, START)
    by_txn = {}
    for e in events:
        by_txn.setdefault(e["transaction_id"], []).append(e["payment_status"])
    assert len(by_txn) == 300
    for statuses in by_txn.values():
        assert len(statuses) == 2
        assert statuses[0] == "INITIATED"
        assert statuses[1] in ("SUCCESS", "FAILED")


def test_event_ids_unique_and_amounts_positive():
    events = generate(300, 42, START)
    ids = [e["event_id"] for e in events]
    assert len(ids) == len(set(ids))
    assert all(e["amount_inr"] > 0 for e in events)
