import os
import sys

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (coalesce, col, current_timestamp, from_json,
                                   from_unixtime)
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

# Parse command line arguments
args = {}
argv = sys.argv[1:]
i = 0
while i < len(argv):
    if argv[i].startswith("--"):
        key = argv[i][2:]
        if i + 1 < len(argv) and not argv[i + 1].startswith("--"):
            args[key] = argv[i + 1]
            i += 2
        else:
            args[key] = "true"
            i += 1
    else:
        i += 1


# MinIO/S3 Configuration - Set BEFORE SparkContext initialization
s3_endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000")
s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "admin123")

# Initialize Spark with Iceberg support - Set Hadoop configs via SparkConf
sc = SparkContext()
spark = (
    SparkSession.builder.appName(args.get("JOB_NAME", "kafka-to-iceberg"))
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.s3a", "org.apache.iceberg.spark.SparkCatalog")
    .config(
        "spark.sql.catalog.s3a.warehouse",
        args.get("iceberg_warehouse", "s3a://warehouse/"),
    )
    .config(
        "spark.sql.catalog.s3a.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"
    )
    .config("spark.sql.catalog.s3a.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.signing.region", "us-east-1")
    .config("spark.sql.catalog.s3a.s3.endpoint", s3_endpoint)
    .config("spark.sql.catalog.s3a.s3.access.key", s3_access_key)
    .config("spark.sql.catalog.s3a.s3.secret.key", s3_secret_key)
    .getOrCreate()
)

print("Starting Kafka to Iceberg pipeline...")
print(f"Kafka brokers: {args.get('kafka_bootstrap_servers')}")
print(f"Topic: {args.get('kafka_topic')}")
print(f"Iceberg warehouse: {args.get('iceberg_warehouse')}")

# Read from Kafka
kafka_options = {
    "kafka.bootstrap.servers": args.get("kafka_bootstrap_servers"),
    "subscribe": args.get("kafka_topic"),
    "startingOffsets": "earliest",
    "kafka.security.protocol": "PLAINTEXT",
}

# Define schema for CDC events
cdc_schema = StructType(
    [
        StructField(
            "before",
            StructType(
                [
                    StructField("order_id", IntegerType()),
                    StructField("customer_id", IntegerType()),
                    StructField("order_date", LongType()),
                    StructField("status", StringType()),
                    StructField("total_amount", DoubleType()),
                    StructField("shipping_address", StringType()),
                ]
            ),
        ),
        StructField(
            "after",
            StructType(
                [
                    StructField("order_id", IntegerType()),
                    StructField("customer_id", IntegerType()),
                    StructField("order_date", LongType()),
                    StructField("status", StringType()),
                    StructField("total_amount", DoubleType()),
                    StructField("shipping_address", StringType()),
                ]
            ),
        ),
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
    ]
)


def process_batch(df, epoch_id):
    if df.count() > 0:
        # Parse Kafka value
        parsed_df = df.select(
            from_json(col("value").cast("string"), cdc_schema).alias("data")
        ).select("data.*")

        # Process based on operation type
        final_df = parsed_df.select(
            coalesce(col("after.order_id"), col("before.order_id")).alias("order_id"),
            col("after.customer_id").alias("customer_id"),
            from_unixtime(col("after.order_date") / 1000)
            .cast("timestamp")
            .alias("order_date"),
            col("after.status").alias("status"),
            col("after.total_amount").alias("total_amount"),
            col("after.shipping_address").alias("shipping_address"),
            col("op").alias("operation"),
            from_unixtime(col("ts_ms") / 1000).cast("timestamp").alias("event_time"),
            current_timestamp().alias("processed_time"),
        ).filter(col("order_id").isNotNull())

        # Write to Iceberg table
        table_name = f"s3a.{args.get('database_name', 'ecommerce')}.{args.get('table_name', 'orders')}"

        # Create table if not exists
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                order_id INT,
                customer_id INT,
                order_date TIMESTAMP,
                status STRING,
                total_amount DOUBLE,
                shipping_address STRING,
                operation STRING,
                event_time TIMESTAMP,
                processed_time TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (days(order_date))
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """
        )

        # Write data
        final_df.writeTo(table_name).option("mergeSchema", "true").append()

        print(f"Processed {final_df.count()} records in epoch {epoch_id}")


# Start streaming query
streaming_df = spark.readStream.format("kafka").options(**kafka_options).load()

checkpoint_location = f"{args.get('iceberg_warehouse', 's3a://warehouse/')}/checkpoints/{args.get('table_name', 'orders')}"

query = (
    streaming_df.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", checkpoint_location)
    .start()
)

print("Streaming query started. Waiting for data...")
query.awaitTermination()
print("Pipeline completed.")
