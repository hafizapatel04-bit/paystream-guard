docker info *> $null
if ($LASTEXITCODE -ne 0) { Write-Host "Docker engine is not running. Start Docker Desktop and retry."; exit 1 }
function Run-Spark {
    docker compose run --rm spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 --conf spark.jars.ivy=/tmp/.ivy @args
}

docker compose up -d
docker exec kafka /opt/kafka/bin/kafka-topics.sh --delete --topic payment-events --bootstrap-server localhost:9092
Start-Sleep -Seconds 5
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic payment-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
Remove-Item data\bronze, data\silver, data\quarantine, data\late, checkpoints -Recurse -Force -ErrorAction SilentlyContinue
docker rm -f bronze-stream 2>$null | Out-Null

python producer\generate_events.py --n-transactions 2000
python producer\fault_injector.py
python producer\kafka_producer.py --rate 500

Write-Host "`n=== Starting bronze stream in small batches ==="
docker compose run -d --name bronze-stream spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 --conf spark.jars.ivy=/tmp/.ivy /app/streaming/bronze_ingest.py --mode stream --max-offsets 500

$marker = "data\bronze\payment_events\_spark_metadata\1"
$deadline = (Get-Date).AddMinutes(5)
while (-not (Test-Path $marker) -and (Get-Date) -lt $deadline) { Start-Sleep -Seconds 1 }

Write-Host "`n=== CRASH: killing the stream mid-ingestion ==="
docker kill bronze-stream
docker rm bronze-stream | Out-Null
python -c "import sys; sys.path.insert(0,'scripts'); from lake_io import read_committed; print('Committed rows at crash:', len(read_committed('data/bronze/payment_events')))"

Write-Host "`n=== RESTART from checkpoint ==="
Run-Spark /app/streaming/bronze_ingest.py
python -c "import pandas as pd; print('Raw rows on disk incl. any orphan files:', len(pd.read_parquet('data/bronze/payment_events')))"

Write-Host "`n=== Silver + reconcile ==="
Run-Spark /app/streaming/silver_job.py
python scripts\reconcile.py

