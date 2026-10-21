import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


spark = (
    SparkSession.builder
    .appName("MarketPulseTradeProcessor")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

trade_schema = StructType([
    StructField("source", StringType()),
    StructField("trade_id", StringType()),
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("size", IntegerType()),
    StructField("event_time", StringType()),
])

raw_trades = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "stock-trades")
    .option("startingOffsets", "earliest")
    .load()
)

parsed_trades = (
    raw_trades
    .select(
        from_json(col("value").cast("string"), trade_schema).alias("trade"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    )
    .select(
        "trade.source",
        "trade.trade_id",
        "trade.symbol",
        "trade.price",
        "trade.size",
        to_timestamp("trade.event_time").alias("event_time"),
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
    )
)

valid_trades = (
    parsed_trades
    .filter(
        col("trade_id").isNotNull()
        & col("symbol").isNotNull()
        & col("price").isNotNull()
        & (col("price") > 0)
        & col("size").isNotNull()
        & (col("size") > 0)
        & col("event_time").isNotNull()
    )
    .withColumn(
        "trade_value",
        spark_round(col("price") * col("size"), 2),
    )
    .withColumn("processed_at", current_timestamp())
    .withColumn(
        "processing_latency_seconds",
        spark_round(
            col("processed_at").cast("double")
            - col("event_time").cast("double"),
            3,
        ),
    )
    .select(
        "trade_id",
        "source",
        "symbol",
        "price",
        "size",
        "trade_value",
        "event_time",
        "kafka_timestamp",
        "processed_at",
        "processing_latency_seconds",
        "kafka_partition",
        "kafka_offset",
    )
)

jdbc_url = (
    f"jdbc:postgresql://postgres:5432/"
    f"{os.environ['POSTGRES_DB']}"
)


def write_to_postgres(batch, _batch_id):
    if batch.isEmpty():
        return

    (
        batch.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "trades")
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .option("numPartitions", "1")
        .option("batchsize", "1000")
        .mode("append")
        .save()
    )


query = (
    valid_trades.writeStream
    .queryName("marketpulse-postgres-writer")
    .foreachBatch(write_to_postgres)
    .option(
        "checkpointLocation",
        "/opt/spark/work-dir/checkpoints/postgres",
    )
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()