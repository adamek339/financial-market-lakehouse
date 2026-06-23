import json
import logging
import os
import requests
import boto3
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)



url = "https://api.nbp.pl/api/exchangerates/tables/A/last/30/?format=json"
response = requests.get(url, timeout=30, verify=False)
response.raise_for_status()
data = response.json()  # lista ~30 elementów


for day in data:
    date = day["effectiveDate"]
    logger.info(f"Saved {date}")
    s3.put_object(
        Bucket="financial-bronze",
        Key=f"nbp/exchange_rates/ingestion_date={date}/data.json",
        Body=json.dumps(day)
      
    )


if __name__ == "__main__":
    logger.info(f"Connecting to MinIO at {MINIO_ENDPOINT}\n")
    logger.info("\nDone.")