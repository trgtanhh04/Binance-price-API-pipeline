from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, TimestampType
from pyspark.sql.functions import col, from_json, to_timestamp, explode, struct, to_json, date_format
import os
from dotenv import load_dotenv
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

CONFIG = {
    "KAFKA_TOPIC_ZSCORE": "btc-price-zscore",
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:29092",
    "MONGO_URI": MONGO_URI,
    "CHECKPOINT_LOCATION": "tmp/checkpoint_zscore_mongo",
    "MONGO_DATABASE": "binance",
    "WINDOWS": ["30s", "1m", "5m", "15m", "30m", "1h"]
}

def init_spark():
    spark = SparkSession.builder \
        .appName("BTC Price Z-Score to MongoDB") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_kafka_topic(spark):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", CONFIG["KAFKA_BOOTSTRAP_SERVERS"]) \
        .option("subscribe", CONFIG["KAFKA_TOPIC_ZSCORE"]) \
        .option("startingOffsets", "earliest") \
        .load()
    return df

def parse_zscore_stream(df):
    window_schema = StructType([
        StructField("window", StringType(), True),
        StructField("zscore_price", DoubleType(), True)
    ])
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("zscores", ArrayType(window_schema), True)
    ])
    df_parsed = (
        df.selectExpr("CAST(value AS STRING)")
          .select(from_json(col("value"), schema).alias("data"))
          .select("data.*")
          .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
          .filter(col("zscores").isNotNull())
          .withWatermark("timestamp", "10 seconds")  
    )
    return df_parsed

def write_to_mongodb(df):
    
    exploded_df = df.select(
        col("timestamp"),
        col("symbol"),
        explode(col("zscores")).alias("zscore")
    ).select(
        col("timestamp"),
        col("symbol"),
        col("zscore.window").alias("window"),
        col("zscore.zscore_price").alias("zscore_price")
    )

    
    windows = CONFIG["WINDOWS"]

    
    for window in windows:
        window_df = exploded_df.filter(col("window") == window)
        query = window_df.writeStream \
            .format("mongodb") \
            .option("spark.mongodb.connection.uri", CONFIG["MONGO_URI"]) \
            .option("spark.mongodb.database", CONFIG["MONGO_DATABASE"]) \
            .option("spark.mongodb.collection", f"btc-price-zscore-{window}") \
            .option("checkpointLocation", f"{CONFIG['CHECKPOINT_LOCATION']}/{window}") \
            .outputMode("append") \
            .start()
        print(f"Started streaming query for collection btc-price-zscore-{window}")

    

def main():
    spark = init_spark()
    raw_df = read_kafka_topic(spark)
    zscore_df = parse_zscore_stream(raw_df)
    write_to_mongodb(zscore_df)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 src/Load/22120017.py

