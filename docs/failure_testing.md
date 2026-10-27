# Failure testing

All data is synthetic. Faults are injected by `producer/fault_injector.py` (seeded, deterministic) and every injected fault is recorded in `data/fault_manifest.json`. `scripts/reconcile.py` checks the pipeline's output against that manifest.

Test setup: 4,000 clean events -> 4,200 events after fault injection, 3-partition Kafka topic, local Docker (single broker, single Spark container).

| Scenario | Injected | Expected behaviour | Observed |
|---|---|---|---|
| Duplicate events | 80 repeated `event_id`s | One copy kept, duplicates dropped | 80 dropped, one copy of each retained, no duplicate `event_id` in silver |
| Invalid amount | 40 negative amounts | Quarantined with reason | 40 in quarantine, reason `invalid_amount`, none in silver |
| Missing transaction ID | 40 events | Quarantined with reason | 40 in quarantine, reason `missing_transaction_id`, none in silver |
| Late events | 120 events, 10-20 min lag | Routed to `late`, excluded from silver | 120 in `late`, ids match manifest exactly |
| Status conflict | 40 transactions get SUCCESS then FAILED | Latest terminal status wins, transaction flagged | 40 flagged of 40 expected |
| Traffic spike | 40 transactions (80 events) from one payer in about a minute | Events preserved | 80 of 80 preserved in silver. No alerting implemented yet |
| Processor crash | Bronze stream killed mid-ingestion | Restart from checkpoint, no loss, no duplicates | TODO: committed rows at crash = ____; after restart 4,200 of 4,200, reconcile 10/10 |

Accounting identity checked on every run: quarantine (80) + late (120) + silver (3,920) + duplicates dropped (80) = bronze (4,200).

## Rules and decisions

- **Late event policy:** `ingestion_time - event_time >= 10 minutes` goes to `late`, measured from the data itself (not Spark's clock) so the result is deterministic.
- **Deduplication:** `dropDuplicatesWithinWatermark` on `event_id`, 30-minute state.
- **Status conflicts:** terminal status beats INITIATED; the latest terminal event wins; conflicting terminals are flagged rather than silently resolved, because there is no source of truth to decide which is correct.

## Limitations

- A duplicate arriving more than 30 minutes after the original would not be dropped.
- Late events are held out of silver, so transactions whose final event is late show as PENDING (103 in this run) until the late table is merged back in. A batch reprocessing job is planned.
- Checkpoints plus the Parquet file sink make bronze writes safe to restart, but this is not end-to-end exactly-once. A killed batch can leave uncommitted orphan files on disk; readers must use the sink's commit log (`scripts/lake_io.py`).
- Single broker, single machine. Throughput figures are for local development hardware only.
