from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

def validate_data(processed_data_path, s3_bucket):
    spark = SparkSession.builder \
        .appName("AdvertiseXDataValidation") \
        .getOrCreate()

    # Load processed data from S3
    processed_data_df = spark.read.parquet(processed_data_path)
    
    # Data Validation Checks
    
    # Check for missing values
    missing_values_df = processed_data_df.select([
        count(when(col(c).isNull(), c)).alias(c) for c in processed_data_df.columns
    ])
    
    missing_values_df.show()

    # Check for duplicates based on primary keys
    primary_keys = ['ad_creative_id', 'user_id', 'timestamp']  # Adjust based on your schema
    duplicates_count = processed_data_df.groupBy(primary_keys).count().filter(col('count') > 1).count()
    
    if duplicates_count > 0:
        print(f"Found {duplicates_count} duplicate rows based on primary keys.")
    else:
        print("No duplicates found based on primary keys.")
    
    # Additional checks can be added as needed
    # For example, checking value ranges, data types, etc.
    
    spark.stop()

