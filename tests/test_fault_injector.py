from datetime import datetime, timezone

from fault_injector import inject
from generate_events import generate

START = datetime(2026, 10, 26, tzinfo=timezone.utc)


def run():
    clean = generate(2000, 42, START)
    out, manifest = inject(clean, 7)
    return clean, out, manifest


def test_fault_sets_are_disjoint():
    _, _, m = run()
    sets = [set(m["invalid_amount"]), set(m["missing_transaction_id"]),
            set(m["late_events"]), set(m["duplicates"])]
    for i in range(len(sets)):
        for j in range(i + 1, len(sets)):
            assert not sets[i] & sets[j]


def test_output_size_adds_up():
    clean, out, m = run()
    expected = (len(clean) + len(m["duplicates"])
                + len(m["status_conflict_transactions"]) + m["spike"]["events"])
    assert len(out) == expected == m["total_output_events"]


def test_expected_counts_match_manifest_lists():
    _, _, m = run()
    e = m["expected"]
    assert e["quarantined"] == len(m["invalid_amount"]) + len(m["missing_transaction_id"])
    assert e["duplicates_to_drop"] == len(m["duplicates"])
    assert e["late_events"] == len(m["late_events"])


def test_injection_is_deterministic():
    clean = generate(2000, 42, START)
    assert inject(clean, 7) == inject(clean, 7)


def test_duplicates_are_exact_copies():
    _, out, m = run()
    for eid in m["duplicates"][:20]:
        copies = [e for e in out if e["event_id"] == eid]
        assert len(copies) == 2 and copies[0] == copies[1]
