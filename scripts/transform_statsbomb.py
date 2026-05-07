import os
import json
import boto3
import logging
import pandas as pd
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_and_upload():
    bucket_name = os.getenv('S3_BUCKET_NAME')
    if not bucket_name:
        logging.error("CRITICAL: S3_BUCKET_NAME is missing from the environment!")
        return

    s3_client = boto3.client('s3')
    raw_key = "raw/statsbomb/competitions.json"
    processed_key = "processed/statsbomb/competitions.parquet"
    
    # We use the container's temporary storage to hold the files during processing
    temp_json_path = "/opt/airflow/raw_data/temp_competitions.json"
    temp_parquet_path = "/opt/airflow/raw_data/temp_competitions.parquet"

    # Step A: Download the raw file from S3 (The "Extract" part of Transformation)
    logging.info(f"Downloading {raw_key} from S3...")
    try:
        s3_client.download_file(bucket_name, raw_key, temp_json_path)
    except ClientError as e:
        logging.error(f"Failed to download raw file from S3: {e}")
        return

    # Step B: Transform the data using Pandas
    logging.info("Transforming JSON data...")
    with open(temp_json_path, 'r') as file:
        data = json.load(file)
    
    # json_normalize flattens nested dictionaries into standard columns!
    df = pd.json_normalize(data)
    
    # Step C: Save as Parquet
    logging.info("Converting DataFrame to Parquet format...")
    df.to_parquet(temp_parquet_path, engine='pyarrow', index=False)
    logging.info(f"Successfully transformed {len(df)} rows.")

    # Step D: Upload the clean Parquet file back to S3
    logging.info(f"Uploading clean data to S3 at {processed_key}...")
    try:
        s3_client.upload_file(temp_parquet_path, bucket_name, processed_key)
        logging.info("SUCCESS: Transformation complete and uploaded!")
    except ClientError as e:
        logging.error(f"Failed to upload processed file to S3: {e}")

if __name__ == "__main__":
    process_and_upload()