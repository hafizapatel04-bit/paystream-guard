from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("transaction_id", StringType()),
    StructField("event_time", StringType()),
    StructField("ingestion_time", StringType()),
    StructField("payer_id", StringType()),
    StructField("amount_inr", DoubleType()),
    StructField("payment_status", StringType()),
    StructField("merchant_category", StringType()),
    StructField("payment_method", StringType()),
    StructField("source_system", StringType()),
])

VALID_STATUSES = ["INITIATED", "SUCCESS", "FAILED"]
TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"
LATE_THRESHOLD_SECONDS = 600  # 10 minutes


def parse_events(bronze):
    """Parse raw_payload and attach reject_reason (null = valid) and lag_seconds."""
    parsed = F.from_json(F.col("raw_payload"), EVENT_SCHEMA)
    flat = (
        bronze.select("raw_payload", "kafka_partition", "kafka_offset", parsed.alias("e"))
        .select("raw_payload", "kafka_partition", "kafka_offset",
                F.col("e").isNull().alias("json_failed"), "e.*")
        .withColumn("event_ts", F.to_timestamp("event_time", TS_FORMAT))
        .withColumn("ingestion_ts", F.to_timestamp("ingestion_time", TS_FORMAT))
        .withColumn("lag_seconds",
                    F.col("ingestion_ts").cast("long") - F.col("event_ts").cast("long"))
    )
    reason = (
        F.when(F.col("json_failed"), "malformed_json")
        .when(F.col("event_id").isNull(), "missing_event_id")
        .when(F.col("transaction_id").isNull() | (F.trim(F.col("transaction_id")) == ""),
              "missing_transaction_id")
        .when(F.col("event_ts").isNull(), "invalid_event_time")
        .when(F.col("ingestion_ts").isNull(), "invalid_ingestion_time")
        .when(F.col("amount_inr").isNull() | (F.col("amount_inr") <= 0), "invalid_amount")
        .when(F.col("payment_status").isNull()
              | ~F.col("payment_status").isin(*VALID_STATUSES), "invalid_status")
    )
    return flat.withColumn("reject_reason", reason)


def split_streams(flat):
    rejected = flat.filter(F.col("reject_reason").isNotNull())
    valid = flat.filter(F.col("reject_reason").isNull())
    late = valid.filter(F.col("lag_seconds") >= LATE_THRESHOLD_SECONDS)
    on_time = valid.filter(F.col("lag_seconds") < LATE_THRESHOLD_SECONDS)
    return rejected, late, on_time


def event_columns(df):
    return df.select(
        "event_id", "transaction_id",
        F.col("event_ts").alias("event_time"),
        F.col("ingestion_ts").alias("ingestion_time"),
        "lag_seconds", "payer_id", "amount_inr", "payment_status",
        "merchant_category", "payment_method", "source_system",
        "kafka_partition", "kafka_offset",
        F.to_date("event_ts").alias("event_date"),
    )


def quarantine_columns(df):
    return df.select("event_id", "reject_reason", "raw_payload", "kafka_partition", "kafka_offset")
