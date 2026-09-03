"""Inspect the Delta Lake telemetry table stored in MinIO."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
DELTA_PATH = os.getenv("DELTA_PATH", "s3a://lakehouse/telemetry")


def build_spark_session() -> SparkSession:
    """Create a local Spark session configured for Delta Lake on MinIO."""
    return (
        SparkSession.builder
        .appName("AV-Telemetry-Reader")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "io.delta:delta-spark_2.12:3.2.1",
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                ]
            ),
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        telemetry = spark.read.format("delta").load(DELTA_PATH)

        print(f"Delta path: {DELTA_PATH}")
        print(f"Rows: {telemetry.count()}")

        print("\nLatest telemetry:")
        telemetry.orderBy(desc("timestamp")).show(20, truncate=False)

        print("\nRows by alert status:")
        telemetry.groupBy("alert").agg(count("*").alias("rows")).show()

        print("\nRows by vehicle:")
        (
            telemetry.groupBy("vehicle_id")
            .agg(count("*").alias("rows"))
            .orderBy(col("rows").desc())
            .show()
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
