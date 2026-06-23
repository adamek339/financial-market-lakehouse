"""
Run once after starting the stack to create MinIO buckets.
Usage: python scripts/init_minio.py
"""

import boto3
from botocore.exceptions import ClientError
import os
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

BUCKETS = [
    "financial-bronze",
    "financial-silver",
    "financial-gold",
]

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)


def create_bucket(bucket_name: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"[SKIP]    {bucket_name} already exists")
    except ClientError:
        s3.create_bucket(Bucket=bucket_name)
        print(f"[CREATED] {bucket_name}")


if __name__ == "__main__":
    print(f"Connecting to MinIO at {MINIO_ENDPOINT}\n")
    for bucket in BUCKETS:
        create_bucket(bucket)
    print("\nDone.")
