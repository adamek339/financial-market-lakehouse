import sys
import os
import logging
import requests
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze.nbp_exchange_rates")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def fetch_nbp_table_a(ingestion_date: str) -> dict:
    url = f"https://api.nbp.pl/api/exchangerates/tables/A/{ingestion_date}/?format=json"
    logger.info(f"Fetching NBP Table A for date: {ingestion_date}")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()[0]


def main(ingestion_date: str) -> None:
    data = fetch_nbp_table_a(ingestion_date)

    spark = get_spark_session()
    df = spark.createDataFrame([data])

    output_path = f"s3a://financial-bronze/nbp/exchange_rates/ingestion_date={ingestion_date}/"
    logger.info(f"Writing {df.count()} record(s) to {output_path}")

    df.write.mode("overwrite").json(output_path)
    logger.info("Done.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Usage: nbp_exchange_rates.py <ingestion_date>")
        sys.exit(1)
    main(sys.argv[1])