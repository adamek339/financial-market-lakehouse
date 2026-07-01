import os
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
BRONZE_PATH = "s3a://financial-bronze/nbp/exchange_rates/"
SILVER_PATH = "s3a://financial-silver/nbp/exchange_rates/"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver.nbp_exchange_rates")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def get_watermark(spark: SparkSession, silver_path: str):
    if not DeltaTable.isDeltaTable(spark, silver_path):
        return None
    return spark.read.format("delta").load(silver_path).agg(F.max("date")).collect()[0][0]

def main():
    start_time = time.time()
    spark = get_spark_session()

    watermark = get_watermark(spark, SILVER_PATH)
    logger.info(f"Watermark: {watermark}")

    df_bronze = spark.read.json(BRONZE_PATH)
    if watermark is not None:
        df_bronze = df_bronze.filter(F.col("ingestion_date") > watermark)

    logger.info(f"Bronze partitions to process: {df_bronze.count()}")

    df_exploded = df_bronze.select(
        F.col("effectiveDate").cast("date").alias("date"),
        F.explode("rates").alias("rate")
    )
    df_flat = df_exploded.select(
        "date",
        F.col("rate.code").alias("currency_code"),
        F.col("rate.currency").alias("currency_name"),
        F.col("rate.mid").cast("decimal(10,4)").alias("rate"),
        F.lit("NBP").alias("source")
    )

    logger.info(f"Exchange rate records to process: {df_flat.count()}")

    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        delta_table = DeltaTable.forPath(spark, SILVER_PATH)
        delta_table.alias("target").merge(
            source=df_flat.alias("source"),
            condition="target.date = source.date AND target.currency_code = source.currency_code"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        logger.info("MERGE completed successfully")
    else:
        df_flat.write.format("delta").save(SILVER_PATH)
        logger.info("Initial write completed successfully")

    logger.info(f"Job completed in {time.time() - start_time:.2f}s")


if __name__ == "__main__":
    main()