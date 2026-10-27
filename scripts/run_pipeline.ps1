function Run-Spark($script) {
    docker compose run --rm spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 --conf spark.jars.ivy=/tmp/.ivy $script
}

docker compose up -d
docker exec kafka /opt/kafka/bin/kafka-topics.sh --delete --topic payment-events --bootstrap-server localhost:9092
Start-Sleep -Seconds 5
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic payment-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
Remove-Item data\bronze, data\silver, data\quarantine, data\late, checkpoints -Recurse -Force -ErrorAction SilentlyContinue

python producer\generate_events.py --n-transactions 2000
python producer\fault_injector.py
python producer\kafka_producer.py --rate 500

Run-Spark /app/streaming/bronze_ingest.py
Run-Spark /app/streaming/silver_job.py
python scripts\reconcile.py
