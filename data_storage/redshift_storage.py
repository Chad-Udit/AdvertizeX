import psycopg2
import boto3
from botocore.exceptions import NoCredentialsError

def load_to_redshift(redshift_endpoint, redshift_user, redshift_password, redshift_db, redshift_table, s3_bucket, s3_file, aws_access_key, aws_secret_key):
    conn = psycopg2.connect(
        dbname=redshift_db,
        user=redshift_user,
        password=redshift_password,
        host=redshift_endpoint,
        port='5439'
    )
    cursor = conn.cursor()
    
    copy_command = f"""
    COPY {redshift_table}
    FROM 's3://{s3_bucket}/{s3_file}'
    ACCESS_KEY_ID '{aws_access_key}'
    SECRET_ACCESS_KEY '{aws_secret_key}'
    FORMAT AS PARQUET;
    """
    
    try:
        cursor.execute(copy_command)
        conn.commit()
        print(f"Data loaded successfully into Redshift table: {redshift_table}")
    except Exception as e:
        print(f"Error loading data into Redshift: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()