# PayStream Guard

Real-time payment data reliability platform (in progress). Simulated UPI-style payment events are streamed through Kafka and processed with PySpark Structured Streaming. All data is synthetic.

## Status
- [x] Seeded event generator and data contract
- [x] Fault injector with a manifest of exactly what was broken
- [x] Kafka producer (keyed by transaction_id, idempotent) and count verifier
- [x] Spark bronze ingestion with checkpoint recovery
- [ ] Validation, quarantine, deduplication, late-event policy (Week 2)
- [ ] S3 lake, dbt models, Airflow orchestration (Week 3)
- [ ] Benchmarks, failure demos, architecture diagram (Week 4)

## Architecture
generator -> fault injector -> Kafka (payment-events, 3 partitions) -> Spark Structured Streaming -> Parquet (bronze)

## Quick start (Windows PowerShell, Docker Desktop, Python 3.11)
    python -m venv .venv
    .venv\Scripts\Activate.ps1
    pip install -r requirements.txt
    docker compose up -d
    python producer\generate_events.py --n-transactions 2000
    python producer\fault_injector.py
    python producer\kafka_producer.py --rate 500
    python producer\kafka_verify.py --expected <total_output_events from data\fault_manifest.json>
    docker compose run --rm spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 --conf spark.jars.ivy=/tmp/.ivy /app/streaming/bronze_ingest.py

## Event schema
See docs/data_contract.md.

## Measured so far
- Producer: 4,200 events delivered at about 500 events/sec, 0 failures (local Docker, single broker)
- Kafka round trip verified against the fault manifest (4,200 in, 4,200 out, 80 injected duplicates present)
- Spark checkpoint: rerun processed 0 new events; 100 added events were picked up exactly once
