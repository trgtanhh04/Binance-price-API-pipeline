from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, avg, stddev, struct, to_json, lit, array, when, date_format, from_utc_timestamp, regexp_replace
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

CONFIG = {
    "KAFKA_TOPIC_PRICE": "btc-price",
    "KAFKA_TOPIC_MOVING": "btc-price-moving",
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:29092"
}

def init_spark():
    spark = SparkSession.builder \
        .appName("BTC Price Transform") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    return spark

def read_topic_kafka(spark):
    kafka_topic = CONFIG["KAFKA_TOPIC_PRICE"]
    kafka_bootstrap_servers = CONFIG["KAFKA_BOOTSTRAP_SERVERS"]

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    return df


def parse_and_watermark(df):
    raw_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("event_time", StringType(), True)
    ])
    df_parsed = (
        df.selectExpr("CAST(value AS STRING)")
          .select(from_json(col("value"), raw_schema).alias("data"))
          .select("data.*")
          .withColumn("timestamp", to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
          .filter(col("price").isNotNull() & (col("price") > 0))
          .withWatermark("timestamp", "10 seconds")
    )
    return df_parsed

def compute_all_stats(df):
    WINDOW_SPECS = [
        ("30s", "30 seconds"),
        ("1m", "1 minute"),
        ("5m", "5 minutes"),
        ("15m", "15 minutes"),
        ("30m", "30 minutes"),
        ("1h", "1 hour"),
    ]
    SLIDE = "10 seconds"
    stats_dfs = []
    for abbr, duration in WINDOW_SPECS:
        stats = (
            df.groupBy(window(col("timestamp"), duration, SLIDE), col("symbol"))
              .agg(
                avg("price").alias(f"avg_{abbr}"),
                stddev("price").alias(f"std_{abbr}")
              )
              .select(
                col("symbol"),
                col("window.end").alias("timestamp"),
                col(f"avg_{abbr}"),
                col(f"std_{abbr}")
              )
        )
        stats_dfs.append((abbr, stats))
    return stats_dfs

def join_and_format(stats_dfs):
    abbrs = [abbr for abbr, df in stats_dfs]
    dfs = [df for abbr, df in stats_dfs]

    joined = dfs[0]
    for i in range(1, len(dfs)):
        joined = joined.join(dfs[i], on=["symbol", "timestamp"], how="inner")

    def window_struct(abbr):
        return struct(
            lit(abbr).alias("window"),
            when(col(f"avg_{abbr}").isNull(), lit(-1)).otherwise(col(f"avg_{abbr}")).alias("avg_price"),
            when(col(f"std_{abbr}").isNull(), lit(-1)).otherwise(col(f"std_{abbr}")).alias("std_price"),
            when(col(f"avg_{abbr}").isNull() | col(f"std_{abbr}").isNull(),
                 lit(f"Warning: not enough data for window {abbr}")
            ).otherwise(lit(None)).alias("warning")
        )

    windows_arr = array(
        *[window_struct(abbr) for abbr in abbrs]
    )

    result = joined.select(
        date_format(from_utc_timestamp(col("timestamp"), "UTC"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("timestamp"),
        col("symbol"),
        windows_arr.alias("windows")
    )
    return result

def format_output(df):
    return df.select(
        col("symbol").cast("string").alias("key"),
        to_json(struct("timestamp", "symbol", "windows")).alias("value"),
    )

def write_to_kafka(df):
    kafka_topic = CONFIG["KAFKA_TOPIC_MOVING"]
    kafka_bootstrap_servers = CONFIG["KAFKA_BOOTSTRAP_SERVERS"]

    query = df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", kafka_topic) \
        .option("checkpointLocation", "/tmp/checkpoint_price_moving") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()
    return query

if __name__ == "__main__":
    spark = init_spark()
    raw_kafka_df = read_topic_kafka(spark)
    parsed_df = parse_and_watermark(raw_kafka_df)
    stats_dfs = compute_all_stats(parsed_df)
    joined_df = join_and_format(stats_dfs)
    formatted_df = format_output(joined_df)
    kafka_query = write_to_kafka(formatted_df)
    kafka_query.awaitTermination()

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 src/Transform/22120017_moving.py
