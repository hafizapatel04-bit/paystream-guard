import json
import sys
from pathlib import Path

import pandas as pd

m = json.loads(Path("data/fault_manifest.json").read_text())
bronze = pd.read_parquet("data/bronze/payment_events")
silver = pd.read_parquet("data/silver/payment_events")
quar = pd.read_parquet("data/quarantine/payment_events")
late = pd.read_parquet("data/late/payment_events")

exp_quar = set(m["invalid_amount"]) | set(m["missing_transaction_id"])
exp_late = set(m["late_events"])
n_dups = len(m["duplicates"])
exp_silver = m["total_output_events"] - len(exp_quar) - len(exp_late) - n_dups

status = silver.groupby("transaction_id")["payment_status"].agg(set)
conflicts = int(status.apply(lambda s: {"SUCCESS", "FAILED"} <= s).sum())

checks = [
    ("bronze rows == events sent", len(bronze) == m["total_output_events"],
     f"{len(bronze)} vs {m['total_output_events']}"),
    ("quarantine ids == manifest (invalid amount + missing txn id)",
     set(quar["event_id"].dropna()) == exp_quar and len(quar) == len(exp_quar),
     f"{len(quar)} vs {len(exp_quar)}"),
    ("late ids == manifest", set(late["event_id"]) == exp_late and len(late) == len(exp_late),
     f"{len(late)} vs {len(exp_late)}"),
    ("silver rows == expected", len(silver) == exp_silver, f"{len(silver)} vs {exp_silver}"),
    ("no duplicate event_id in silver", silver["event_id"].is_unique, ""),
    ("one copy of every duplicated event kept", set(m["duplicates"]) <= set(silver["event_id"]), f"{n_dups} duplicated ids"),
    ("no bad amounts / missing txn ids in silver",
     bool((silver["amount_inr"] > 0).all() and silver["transaction_id"].notna().all()), ""),
    ("every bronze row accounted for", len(quar) + len(late) + len(silver) + n_dups == len(bronze),
     f"{len(quar)}+{len(late)}+{len(silver)}+{n_dups} = {len(quar)+len(late)+len(silver)+n_dups}"),
    ("spike payer events preserved", int((silver["payer_id"] == "acc_SPIKE").sum()) == m["spike"]["events"],
     f"{int((silver['payer_id'] == 'acc_SPIKE').sum())} vs {m['spike']['events']}"),
    ("status-conflict transactions detected", conflicts == len(m["status_conflict_transactions"]),
     f"{conflicts} vs {len(m['status_conflict_transactions'])}"),
]

print("\nQuarantine reasons:", quar["reject_reason"].value_counts().to_dict())
print()
failed = 0
for name, ok, detail in checks:
    print(f"[{'PASS' if ok else 'FAIL'}] {name}  {detail}")
    failed += (not ok)
print(f"\n{len(checks) - failed}/{len(checks)} checks passed")
sys.exit(1 if failed else 0)
