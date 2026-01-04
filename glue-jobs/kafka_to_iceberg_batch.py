"""
Batch Kafka to Iceberg Job
Processes existing Kafka messages and writes to S3/MinIO via Iceberg
"""
import sys
import os
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Parse arguments
args = {}
argv = sys.argv[1:]
i = 0
while i < len(argv):
    if argv[i].startswith('--'):
        key = argv[i][2:]
        if i + 1 < len(argv) and not argv[i+1].startswith('--'):
            args[key] = argv[i+1]
            i += 2
        else:
            args[key] = 'true'
            i += 1
    else:
        i += 1

# Initialize Spark with Iceberg support
sc = SparkContext()
spark = SparkSession.builder \
    .appName(args.get('JOB_NAME', 'kafka-to-iceberg-batch')) \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.s3a", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.s3a.warehouse", args.get('iceberg_warehouse', 's3a://warehouse/')) \
    .getOrCreate()

# MinIO/S3 Configuration
s3_endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000")
s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "admin123")

spark.conf.set("spark.sql.catalog.s3a.s3.endpoint", s3_endpoint)
spark.conf.set("spark.sql.catalog.s3a.s3.access.key", s3_access_key)
spark.conf.set("spark.sql.catalog.s3a.s3.secret.key", s3_secret_key)
spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

print("Starting Batch Kafka to Iceberg pipeline...")
print(f"Kafka brokers: {args.get('kafka_bootstrap_servers')}")
print(f"Topic: {args.get('kafka_topic')}")
print(f"Iceberg warehouse: {args.get('iceberg_warehouse')}")

# Read batch from Kafka
kafka_options = {
    "kafka.bootstrap.servers": args.get('kafka_bootstrap_servers'),
    "subscribe": args.get('kafka_topic'),
    "startingOffsets": "earliest",
    "endingOffsets": "latest",
    "kafka.security.protocol": "PLAINTEXT"
}

# Define schema for CDC events
cdc_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("shipping_address", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True)
])

# Read from Kafka
df = spark.read.format("kafka") \
    .options(**kafka_options) \
    .load()

print(f"Read {df.count()} messages from Kafka")

if df.count() > 0:
    # Parse the JSON values
    parsed_df = df.select(
        from_json(col("value").cast("string"), cdc_schema).alias("data")
    ).select("data.*")

    # Transform data
    final_df = parsed_df.select(
        col("order_id"),
        col("customer_id"),
        col("order_date").cast("string").alias("order_date"),
        col("status"),
        col("total_amount").cast("double").alias("total_amount"),
        col("shipping_address"),
        col("created_at").cast("string").alias("created_at"),
        col("updated_at").cast("string").alias("updated_at"),
        current_timestamp().alias("processed_time")
    ).filter(col("order_id").isNotNull())

    print(f"Parsed {final_df.count()} valid records")

    # Create Iceberg table
    table_name = f"s3a.{args.get('database_name', 'ecommerce')}.{args.get('table_name', 'orders')}"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            order_id INT,
            customer_id INT,
            order_date STRING,
            status STRING,
            total_amount DOUBLE,
            shipping_address STRING,
            created_at STRING,
            updated_at STRING,
            processed_time TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)

    # Write data (overwrite mode to avoid duplicates in batch mode)
    final_df.writeTo(table_name).option("mergeSchema", "true").overwritePartitions()

    msg = f"Successfully wrote {final_df.count()} records to Iceberg table: {table_name}"
    print(msg)

    # Show sample data
    print("\nSample data written:")
    spark.sql(f"SELECT * FROM {table_name} LIMIT 5").show()
else:
    print("No messages found in Kafka topic")

print("Batch job completed!")
spark.stop()

