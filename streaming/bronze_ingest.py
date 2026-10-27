import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="kafka:29092")
    p.add_argument("--topic", default="payment-events")
    p.add_argument("--out", default="/app/data/bronze/payment_events")
    p.add_argument("--checkpoint", default="/app/checkpoints/bronze")
    p.add_argument("--mode", choices=["drain", "stream"], default="drain",
                   help="drain = process everything available then stop; stream = run continuously")
    p.add_argument("--max-offsets", type=int, default=0,
                   help="cap events per micro-batch (0 = no cap)")
    a = p.parse_args()

    spark = (SparkSession.builder.appName("paystream-bronze")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", a.bootstrap)
              .option("subscribe", a.topic)
              .option("startingOffsets", "earliest")
              .option("failOnDataLoss", "false"))
    if a.max_offsets > 0:
        reader = reader.option("maxOffsetsPerTrigger", a.max_offsets)
    raw = reader.load()

    bronze = (raw.select(
                F.col("value").cast("string").alias("raw_payload"),
                F.col("key").cast("string").alias("kafka_key"),
                F.col("topic").alias("kafka_topic"),
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                F.col("timestamp").alias("kafka_timestamp"))
              .withColumn("ingest_date", F.to_date("kafka_timestamp")))

    writer = (bronze.writeStream.format("parquet")
              .option("path", a.out)
              .option("checkpointLocation", a.checkpoint)
              .partitionBy("ingest_date")
              .outputMode("append"))
    writer = (writer.trigger(availableNow=True) if a.mode == "drain"
              else writer.trigger(processingTime="5 seconds"))

    query = writer.start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
