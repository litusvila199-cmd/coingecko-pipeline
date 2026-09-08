import requests
import boto3
import json

from datetime import datetime, timezone
from src.utils.logger import get_logger
from src.config.settings import API_URL, CRYPTO_IDS, S3_BUCKET

logger = get_logger(__name__)

s3 = boto3.client("s3")

def extract_data():
    try:
        response = requests.get(
            API_URL,
            params={
                "ids": ",".join(CRYPTO_IDS),
                "vs_currencies": "usd"
            },
            timeout=10
        )
        response.raise_for_status()

        data = response.json()

        logger.info("Data extracted successfully")

        return data

    except requests.RequestException as e:
        logger.error(f"Error extracting data: {e}")
        raise

def save_to_s3(data):
    json_data = json.dumps(data)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d/%H-%M-%S")

    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"coingecko/raw/{timestamp}/prices.json",
            Body=json_data,
            ContentType="application/json"
        )

        logger.info("Data saved to S3 successfully")

    except Exception as e:
        logger.error(f"Error saving data to S3: {e}")
        raise    



if __name__ == "__main__":
    data = extract_data()
    save_to_s3(data)