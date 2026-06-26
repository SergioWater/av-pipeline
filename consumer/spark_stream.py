"""
Stage 4 — STORE: Spark writes the GOOD stream to a Delta Lake table on MinIO.

GOAL OF THIS FILE:
    Same pipeline as 3.6 (good → console, bad → DLQ) PLUS a new third
    output: persist GOOD messages durably as a Delta table living in
    s3a://lakehouse/telemetry inside MinIO.

THE ULTIMATE OUTPUT:
    - Console alert table (live view)
    - Dead-letter-queue topic in Redpanda (broken messages)
    - Durable Delta table at s3a://lakehouse/telemetry  ← NEW

HOW TO RUN (3 terminals, all with `conda activate avpipe`):
    Terminal 1:  python producer/simulator.py
    Terminal 2:  python consumer/spark_stream.py
    Terminal 3:  open http://localhost:9001 in browser to SEE the files
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# ---------------------------------------------------------------------
# 1. START SPARK  -- now with Kafka + Delta + S3A all wired in
# ---------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("AV-Telemetry-Processor")
    # --- JARs: Kafka (already had it) + Delta + Hadoop-AWS (new) ---
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
            "io.delta:delta-spark_2.12:3.2.1",
            "org.apache.hadoop:hadoop-aws:3.3.4",
        ]),
    )
    # --- Turn ON Delta Lake support ---
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    # --- S3A: where MinIO lives + credentials + MinIO quirks ---
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")   # MinIO needs this
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")  # plain HTTP
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------
# 2. SCHEMA
# ---------------------------------------------------------------------
schema = StructType([
    StructField("vehicle_id",     StringType()),
    StructField("timestamp",      StringType()),
    StructField("latitude",       DoubleType()),
    StructField("longitude",      DoubleType()),
    StructField("speed_mph",      DoubleType()),
    StructField("battery_temp_c", DoubleType()),
    StructField("sensor_status",  StructType([
        StructField("lidar",  StringType()),
        StructField("camera", StringType()),
        StructField("radar",  StringType()),
    ])),
])

# ---------------------------------------------------------------------
# 3. READ FROM REDPANDA
# ---------------------------------------------------------------------
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:19092")
    .option("subscribe", "raw-vehicle-telemetry")
    .option("startingOffsets", "earliest")
    .load()
)

# ---------------------------------------------------------------------
# 4. PARSE  (keep raw_json so we can quarantine broken messages)
# ---------------------------------------------------------------------
parsed = (
    raw
    .selectExpr("CAST(value AS STRING) AS raw_json")
    .withColumn("d", from_json(col("raw_json"), schema))
    .select("raw_json", "d.*")
)

# ---------------------------------------------------------------------
# 5. SPLIT GOOD vs BAD
# ---------------------------------------------------------------------
good_df = parsed.filter(col("vehicle_id").isNotNull())
bad_df  = parsed.filter(col("vehicle_id").isNull())

# ---------------------------------------------------------------------
# 6. GOOD STREAM #1 — apply alert rule, print to console
# ---------------------------------------------------------------------
flagged = good_df.withColumn(
    "alert",
    when(
        (col("sensor_status.lidar")  == "FAILED") |
        (col("sensor_status.camera") == "FAILED") |
        (col("sensor_status.radar")  == "FAILED") |
        (col("battery_temp_c") > 45),
        "ALERT",
    ).otherwise("OK")
)

console_query = (
    flagged.select(
        "alert", "vehicle_id", "speed_mph", "battery_temp_c", "sensor_status"
    )
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/checkpoints/good")
    .start()
)

# ---------------------------------------------------------------------
# 7. GOOD STREAM #2 — NEW: write durably to Delta Lake on MinIO
#    We drop raw_json (don't need the original text in the warehouse).
# ---------------------------------------------------------------------
delta_query = (
    flagged
    .select(
        "vehicle_id", "timestamp", "latitude", "longitude",
        "speed_mph", "battery_temp_c", "sensor_status", "alert"
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/delta")
    .start("s3a://lakehouse/telemetry")
)

# ---------------------------------------------------------------------
# 8. BAD STREAM — DLQ (unchanged from 3.6)
# ---------------------------------------------------------------------
dlq_query = (
    bad_df
    .selectExpr("CAST(raw_json AS STRING) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:19092")
    .option("topic", "dead-letter-queue")
    .option("checkpointLocation", "/tmp/checkpoints/dlq")
    .start()
)

# Keep all three streams alive.
spark.streams.awaitAnyTermination()