import json
import sys
from pathlib import Path

import pandas as pd

TERMINAL = ["SUCCESS", "FAILED"]


def resolve_transactions(events):
    """One row per transaction. Terminal status beats INITIATED; latest terminal event wins;
    conflicting terminal statuses are flagged, not hidden."""
    df = events.copy()
    df["is_terminal"] = df["payment_status"].isin(TERMINAL)

    ordered = df.sort_values(["transaction_id", "is_terminal", "event_time", "kafka_offset"])
    latest = ordered.groupby("transaction_id", as_index=False).tail(1)
    latest = latest[["transaction_id", "payer_id", "amount_inr",
                     "merchant_category", "payment_status"]]
    latest = latest.rename(columns={"payment_status": "final_status"})
    latest["final_status"] = latest["final_status"].replace({"INITIATED": "PENDING"})

    summary = (df.groupby("transaction_id")
               .agg(first_event_time=("event_time", "min"),
                    last_event_time=("event_time", "max"),
                    n_events=("event_id", "count"))
               .reset_index())
    terminal = (df[df["is_terminal"]].groupby("transaction_id")["payment_status"]
                .nunique().rename("n_terminal_statuses").reset_index())

    out = latest.merge(summary, on="transaction_id").merge(terminal, on="transaction_id", how="left")
    out["n_terminal_statuses"] = out["n_terminal_statuses"].fillna(0).astype(int)
    out["status_conflict"] = out["n_terminal_statuses"] > 1
    out = out.drop(columns="n_terminal_statuses")
    return out.sort_values("transaction_id").reset_index(drop=True)


if __name__ == "__main__":
    from lake_io import read_committed

    silver = read_committed("data/silver/payment_events")
    txns = resolve_transactions(silver)
    Path("data/gold").mkdir(parents=True, exist_ok=True)
    txns.to_parquet("data/gold/transactions.parquet", index=False)

    manifest = json.loads(Path("data/fault_manifest.json").read_text())
    expected = len(manifest["status_conflict_transactions"])
    found = int(txns["status_conflict"].sum())
    print(f"Transactions: {len(txns)}")
    print(f"Final statuses: {txns['final_status'].value_counts().to_dict()}")
    print(f"Status conflicts flagged: {found} (manifest expects {expected})")
    print("PASS" if found == expected else "FAIL")
    sys.exit(0 if found == expected else 1)
