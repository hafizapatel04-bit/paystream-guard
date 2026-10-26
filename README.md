PayStream Guard

Real-Time Payment Data Reliability Platform

PayStream Guard is a data engineering project that simulates UPI-style payment events and processes them through a Kafka and PySpark Structured Streaming pipeline. The project focuses on reliable ingestion, fault injection, checkpoint recovery, and data quality.

Status: In Progress

Architecture
text
Event Generator → Fault Injector → Apache Kafka
                                       |
                                       v
                              Spark Structured Streaming
                                       |
                                       v
                                Parquet (Bronze)
                                       |
                                       v
                          Validation & Deduplication

Tech Stack

Python · Apache Kafka · PySpark · Structured Streaming · Parquet · Docker

Key Features
Generated synthetic payment events with a defined data contract.
Injected controlled data defects and tracked them using a fault manifest.
Published events to Kafka using transaction_id as the message key.
Verified Kafka delivery counts against the fault manifest.
Implemented a Spark bronze ingestion job with Parquet output and checkpointing.
Tested checkpoint recovery and incremental event ingestion.
Verified Results
Metric	Result
Events delivered	4,200
Injected duplicates	80
Delivery failures	0
Kafka partitions	3
Producer throughput	~500 events/sec
Incremental events tested	100

Results measured in a local Docker environment with a single Kafka broker.

Roadmap
Event generation and fault injection
Kafka producer and delivery verification
Spark bronze ingestion
Checkpoint recovery testing
Schema validation and quarantine
Deduplication and late-event handling
S3 data lake and dbt transformations
Airflow orchestration and performance benchmarks
Run Locally

Prerequisites: Python 3.11 and Docker Desktop.

powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
docker compose up -d


Generate events and inject faults:

powershell
python producer\generate_events.py --n-transactions 2000
python producer\fault_injector.py


Publish and verify:

powershell
python producer\kafka_producer.py --rate 500
python producer\kafka_verify.py --expected <total_output_events>


Run the Spark bronze job:

powershell
docker compose run --rm spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 --conf spark.jars.ivy=/tmp/.ivy /app/streaming/bronze_ingest.py


Replace <total_output_events> with the value recorded in data/fault_manifest.json.

Data Contract

See docs/data_contract.md for the event schema and field definitions.

Current Focus

Building a reliable streaming pipeline with verifiable ingestion, recoverable processing, and a clear path toward production-style data quality and orchestration.

Project status: Active development. All payment data is synthetic.
