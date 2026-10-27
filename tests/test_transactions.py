import pandas as pd

from transactions import resolve_transactions


def ev(event_id, txn, status, t, offset=0):
    return {"event_id": event_id, "transaction_id": txn, "payment_status": status,
            "event_time": pd.Timestamp(t, tz="UTC"), "kafka_offset": offset,
            "payer_id": "acc_1", "amount_inr": 100.0, "merchant_category": "FOOD"}


def one(df, txn):
    return df.set_index("transaction_id").loc[txn]


def test_normal_success_flow():
    df = resolve_transactions(pd.DataFrame([
        ev("e1", "t1", "INITIATED", "2026-10-26 10:00:00"),
        ev("e2", "t1", "SUCCESS", "2026-10-26 10:00:03")]))
    r = one(df, "t1")
    assert r.final_status == "SUCCESS"
    assert not r.status_conflict
    assert r.n_events == 2


def test_only_initiated_is_pending():
    df = resolve_transactions(pd.DataFrame([ev("e1", "t1", "INITIATED", "2026-10-26 10:00:00")]))
    assert one(df, "t1").final_status == "PENDING"


def test_conflicting_terminal_statuses_latest_wins_and_is_flagged():
    df = resolve_transactions(pd.DataFrame([
        ev("e1", "t1", "INITIATED", "2026-10-26 10:00:00"),
        ev("e2", "t1", "SUCCESS", "2026-10-26 10:00:03"),
        ev("e3", "t1", "FAILED", "2026-10-26 10:00:05")]))
    r = one(df, "t1")
    assert r.final_status == "FAILED"
    assert r.status_conflict


def test_terminal_beats_initiated_even_if_initiated_timestamp_is_later():
    df = resolve_transactions(pd.DataFrame([
        ev("e1", "t1", "SUCCESS", "2026-10-26 10:00:03"),
        ev("e2", "t1", "INITIATED", "2026-10-26 10:00:09")]))
    r = one(df, "t1")
    assert r.final_status == "SUCCESS"
    assert not r.status_conflict


def test_result_does_not_depend_on_arrival_order():
    rows = [ev("e1", "t1", "INITIATED", "2026-10-26 10:00:00"),
            ev("e2", "t1", "SUCCESS", "2026-10-26 10:00:03"),
            ev("e3", "t1", "FAILED", "2026-10-26 10:00:05")]
    a = resolve_transactions(pd.DataFrame(rows))
    b = resolve_transactions(pd.DataFrame(rows[::-1]))
    pd.testing.assert_frame_equal(a, b)
