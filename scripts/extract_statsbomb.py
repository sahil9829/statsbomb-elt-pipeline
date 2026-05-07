import requests
import json
import os
import boto3
import logging
from botocore.exceptions import ClientError

# Set up logging so Airflow captures our messages properly
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_competitions():
    url = "https://raw.githubusercontent.com/statsbomb/open-data/master/data/competitions.json"
    logging.info("Connecting to StatsBomb API...")
    
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        logging.info(f"Success! Fetched data for {len(data)} competitions.")
        
        save_path = os.path.join("..", "raw_data", "competitions.json")
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        
        with open(save_path, 'w') as file:
            json.dump(data, file, indent=4)
            
        logging.info(f"Data temporarily staged at {save_path}")
        return save_path
    else:
        logging.error(f"Failed to fetch data. Error code: {response.status_code}")
        return None

def upload_to_s3(local_file_path):
    bucket_name = os.getenv('S3_BUCKET_NAME')
    
    if not bucket_name:
        logging.error("CRITICAL: S3_BUCKET_NAME is missing from the environment!")
        return
    
    # Define what the file will be called inside the AWS S3 bucket
    s3_file_key = "raw/statsbomb/competitions.json"
    
    logging.info(f"Attempting to upload {local_file_path} to S3 bucket: {bucket_name}...")
    
    # Boto3 automatically finds the AWS keys hidden in our .env file!
    s3_client = boto3.client('s3')
    
    try:
        s3_client.upload_file(local_file_path, bucket_name, s3_file_key)
        logging.info(f"SUCCESS: File uploaded to s3://{bucket_name}/{s3_file_key}")
    except ClientError as e:
        logging.error(f"AWS Upload Failed: {e}")

if __name__ == "__main__":
    # 1. Fetch the data and save locally
    staged_file = fetch_competitions()
    
    # 2. If step 1 worked, push it to the cloud
    if staged_file:
        upload_to_s3(staged_file)