from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Define schemas for JSON and CSV data
ad_impressions_schema = StructType([
    StructField("ad_creative_id", StringType()),
    StructField("user_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("website", StringType())
])

clicks_conversions_schema = StructType([
    StructField("event_timestamp", TimestampType()),
    StructField("user_id", StringType()),
    StructField("ad_campaign_id", StringType()),
    StructField("conversion_type", StringType())
])

bid_requests_schema = StructType([
    StructField("user_id", StringType()),
    StructField("auction_details", StringType()),
    StructField("ad_targeting_criteria", StringType())
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AdvertiseXStreamingProcessing") \
    .getOrCreate()

# Read streaming data from Kafka
ad_impressions_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ad_impressions") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", ad_impressions_schema).alias("data")) \
    .select("data.*")

clicks_conversions_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clicks_conversions") \
    .load() \
    .selectExpr("CAST(value AS STRING) as csv") \
    .select(from_json("csv", clicks_conversions_schema).alias("data")) \
    .select("data.*")

bid_requests_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bid_requests") \
    .load() \
    .selectExpr("CAST(value AS STRING) as avro") \
    .select(from_json("avro", bid_requests_schema).alias("data")) \
    .select("data.*")

# Join ad impressions with clicks and conversions
ad_clicks_conversions_df = ad_impressions_df \
    .join(clicks_conversions_df, expr("""
        ad_impressions_df.user_id = clicks_conversions_df.user_id AND
        ad_impressions_df.timestamp <= clicks_conversions_df.event_timestamp AND
        ad_impressions_df.timestamp >= clicks_conversions_df.event_timestamp - interval 1 hour
    """), "leftOuter")

# Write the output to S3
query = ad_clicks_conversions_df \
    .writeStream \
    .format("parquet") \
    .option("path", "s3://advertisex/processed/ad_clicks_conversions/") \
    .option("checkpointLocation", "s3://advertisex/checkpoints/ad_clicks_conversions/") \
    .outputMode("append") \
    .start()

query.awaitTermination()
