import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, LongType, StringType, StructField,
                               StructType, TimestampType)

from validation_rules import event_columns, parse_events, quarantine_columns, split_streams

BRONZE_SCHEMA = StructType([
    StructField("raw_payload", StringType()),
    StructField("kafka_key", StringType()),
    StructField("kafka_topic", StringType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
    StructField("kafka_timestamp", TimestampType()),
])


def start(df, path, checkpoint, mode, partition_by=None):
    w = (df.writeStream.format("parquet").option("path", path)
         .option("checkpointLocation", checkpoint).outputMode("append"))
    if partition_by:
        w = w.partitionBy(partition_by)
    w = w.trigger(availableNow=True) if mode == "drain" else w.trigger(processingTime="5 seconds")
    return w.start()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bronze", default="/app/data/bronze/payment_events")
    p.add_argument("--mode", choices=["drain", "stream"], default="drain")
    p.add_argument("--dedupe-window", default="30 minutes")
    a = p.parse_args()

    spark = (SparkSession.builder.appName("paystream-silver")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.readStream.schema(BRONZE_SCHEMA).parquet(a.bronze)
    rejected, late, on_time = split_streams(parse_events(bronze))

    silver = (event_columns(on_time)
              .withWatermark("event_time", a.dedupe_window)
              .dropDuplicatesWithinWatermark(["event_id"]))

    queries = [
        start(silver, "/app/data/silver/payment_events", "/app/checkpoints/silver", a.mode, "event_date"),
        start(quarantine_columns(rejected), "/app/data/quarantine/payment_events", "/app/checkpoints/quarantine", a.mode),
        start(event_columns(late), "/app/data/late/payment_events", "/app/checkpoints/late", a.mode),
    ]
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
