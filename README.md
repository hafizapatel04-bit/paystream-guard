**PayStream Guard

A Real-Time Payment Data Reliability Platform

Project status: In progress

PayStream Guard is a data engineering project that simulates UPI-style payment events and processes them through a real-time streaming pipeline. It focuses on data quality, fault injection, reliable ingestion, checkpoint recovery, and event-level traceability.

All payment events are synthetic and intended for development and demonstration purposes.

Project Overview

PayStream Guard demonstrates how a modern streaming data pipeline can ingest payment events, preserve raw records, and build toward reliable downstream data processing.

Architecture
Synthetic Event Generator
          |
          v
     Fault Injector
          |
          v
    Apache Kafka
  payment-events
   3 partitions
          |
          v
  PySpark Structured
       Streaming
          |
          v
   Parquet Data Lake
      Bronze Layer
          |
          v
 Validation & Deduplication
       (Week 2)


Implementation Status
Week 1 — Event Generation & Ingestion
Synthetic payment event generator
Data contract and event schema
Fault injector with a manifest of injected defects
Kafka producer keyed by transaction_id
Kafka delivery count verification
Spark bronze ingestion with Parquet output
Checkpoint recovery and incremental ingestion verification
Week 2 — Data Quality & Reliability
Event validation and schema enforcement
Invalid-event quarantine
Duplicate detection and deduplication
Late-event handling policy
Week 3 — Data Lake & Orchestration
S3-compatible data lake
dbt transformation models
Airflow orchestration
Week 4 — Testing & Demonstration
Throughput and performance benchmarks
Failure recovery demonstrations
Architecture diagram and technical documentation
Technology Stack
Technology	Purpose
Python 3.11	Event generation and producer tooling
Apache Kafka	Distributed event streaming
PySpark Structured Streaming	Stream ingestion and processing
Parquet	Columnar storage for the bronze layer
Docker Compose	Local development environment
AWS S3 / S3-compatible storage	Planned data lake
dbt	Planned data transformations
Apache Airflow	Planned workflow orchestration
Verified Results

The following results have been observed in the local Docker environment.

Metric	Result
Events delivered to Kafka	4,200
Events verified against manifest	4,200
Injected duplicate events preserved	80
Kafka partitions	3
Producer throughput	~500 events/sec
Producer delivery failures	0
New events picked up after checkpoint restart	100

Checkpoint recovery test: Re-running the Spark ingestion job with the same checkpoint processed no new events. After adding 100 events, the pipeline picked up those new events, bringing the expected bronze count from 4,200 to 4,300.

These measurements reflect a local development setup with a single Kafka broker, not a production benchmark.

Quick Start
Prerequisites
Windows with PowerShell
Docker Desktop with at least 4 GB of memory allocated
Python 3.11
Git
1. Set up Python
powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt

2. Start Kafka
powershell
docker compose up -d

3. Generate synthetic events
powershell
python producer\generate_events.py --n-transactions 2000
python producer\fault_injector.py

4. Publish and verify events
powershell
python producer\kafka_producer.py --rate 500
python producer\kafka_verify.py --expected <total_output_events>


Replace <total_output_events> with the value of total_output_events in data\fault_manifest.json.

5. Run Spark bronze ingestion
powershell
docker compose run --rm spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 --conf spark.jars.ivy=/tmp/.ivy /app/streaming/bronze_ingest.py


The job reads events from Kafka and writes the raw payload and Kafka metadata to partitioned Parquet files. Its checkpoint tracks streaming progress across restarts.

Data Contract

The payment event schema and field definitions are documented in docs/data_contract.md.

The bronze layer preserves the original event payload alongside Kafka metadata, providing a foundation for subsequent validation, quarantine, and deduplication.

Engineering Focus
Data reliability: Verify delivered event counts against an explicit fault manifest.
Traceability: Retain Kafka topic, partition, offset, timestamp, and raw payload.
Recovery: Use Structured Streaming checkpoints to resume ingestion.
Data quality: Introduce validation and deduplication as separate downstream stages.
Reproducibility: Run the local pipeline with Docker and synthetic data.
Project Status

PayStream Guard is an active portfolio project. The initial Kafka ingestion and bronze-layer recovery milestones have been tested locally. Data quality processing, lakehouse transformations, orchestration, and performance evaluation remain planned milestones.

Next milestone: Complete Week 2 validation, quarantine, deduplication, and late-event handling.

Built as a hands-on data engineering project focused on streaming reliability and data quality.
**
