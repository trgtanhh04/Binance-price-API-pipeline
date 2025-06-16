from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, explode, to_json, udf, collect_list, to_timestamp, to_utc_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
import logging

CONFIG = {
    "KAFKA_TOPIC_PRICE": "btc-price",
    "KAFKA_TOPIC_MOVING": "btc-price-moving",
    "KAFKA_TOPIC_ZSCORE": "btc-price-zscore",
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:29092",
}

def init_spark():
    spark = SparkSession.builder \
        .appName("BTC Price Z-Score") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    return spark

def get_schemas():
    price_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("event_time", StringType(), True)
    ])

    window_schema = StructType([
        StructField("window", StringType(), True),
        StructField("avg_price", DoubleType(), True),
        StructField("std_price", DoubleType(), True)
    ])

    moving_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("windows", ArrayType(window_schema), True)
    ])
    return price_schema, moving_schema



def read_kafka_streams(spark, price_schema, moving_schema):
    # Read btc-price
    df_price = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", CONFIG["KAFKA_BOOTSTRAP_SERVERS"]) \
        .option("subscribe", CONFIG["KAFKA_TOPIC_PRICE"]) \
        .option("startingOffsets", "earliest") \
        .load()

    df_price = df_price.select(from_json(col("value").cast("string"), price_schema).alias("data")) \
        .select(
            col("data.symbol").alias("symbol"),
            col("data.price").alias("price"),
            to_timestamp(col("data.event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSX").alias("timestamp")
        ).withWatermark("timestamp", "2 minutes")

    # Read btc-price-moving
    df_moving = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", CONFIG["KAFKA_BOOTSTRAP_SERVERS"]) \
        .option("subscribe", CONFIG["KAFKA_TOPIC_MOVING"]) \
        .option("startingOffsets", "earliest") \
        .load()

    df_moving = df_moving.select(from_json(col("value").cast("string"), moving_schema).alias("data")) \
        .select(
            col("data.symbol").alias("symbol"),
            to_timestamp(col("data.timestamp"), "yyyy-MM-dd'T'HH:mm:ssX").alias("timestamp"),
            col("data.windows").alias("windows")
        ).withWatermark("timestamp", "2 minutes")

    return df_price, df_moving

def calculate_zscore(price, avg, std):
    if std is None or std == 0:
        return None
    return float((price - avg) / std)

calculate_zscore_udf = udf(calculate_zscore, DoubleType())


def compute_zscore(df_price, df_moving):
    # Join stream-stream
    joined = df_price.join(df_moving, on=["symbol", "timestamp"], how="inner")

    exploded = joined.withColumn("window", explode("windows"))

    result = exploded.select(
        "timestamp",
        "symbol",
        struct(
            col("window.window").alias("window"),
            calculate_zscore_udf(col("price"), col("window.avg_price"), col("window.std_price")).alias("zscore_price")
        ).alias("zscore")
    )

    final = result.groupBy("symbol", "timestamp") \
        .agg(collect_list("zscore").alias("zscores")) \
        .select("timestamp", "symbol", "zscores")

    return final

def format_output(final):
    return final.select(
        col("symbol").alias("key"),
        to_json(
            struct(
                date_format(to_utc_timestamp(col("timestamp"), "Asia/Ho_Chi_Minh"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("timestamp"),
                col("symbol"),
                col("zscores")
            )
        ).alias("value")
    )

def write_to_kafka(df):
    kafka_topic = CONFIG["KAFKA_TOPIC_ZSCORE"]
    kafka_bootstrap_servers = CONFIG["KAFKA_BOOTSTRAP_SERVERS"]
    query = df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", kafka_topic) \
        .option("checkpointLocation", "/tmp/checkpoint_btc_price_zscore") \
        .outputMode("append") \
        .start()
    return query


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    spark = init_spark()
    price_schema, moving_schema = get_schemas()
    df_price, df_moving = read_kafka_streams(spark, price_schema, moving_schema)
    final = compute_zscore(df_price, df_moving)
    formatted = format_output(final)

    query = write_to_kafka(formatted)

    query.awaitTermination()

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 src/Transform/22120017_zscore.py

# Note: xóa /tmp/checkpoint_btc_price_zscore trước khi chạy