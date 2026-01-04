"""
Simple Batch Kafka to Parquet Job
Processes Kafka messages and writes to local/parquet format (no Iceberg for testing)
"""
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import sys
import os

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

# Initialize Spark
sc = SparkContext()
spark = SparkSession.builder \
    .appName(args.get('JOB_NAME', 'kafka-to-parquet')) \
    .getOrCreate()

print("Starting Kafka to Parquet pipeline...")
print(f"Kafka brokers: {args.get('kafka_bootstrap_servers')}")
print(f"Topic: {args.get('kafka_topic')}")

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
        col("order_date"),
        col("status"),
        col("total_amount").cast("double").alias("total_amount"),
        col("shipping_address"),
        col("created_at"),
        col("updated_at"),
        current_timestamp().alias("processed_time")
    ).filter(col("order_id").isNotNull())

    print(f"Parsed {final_df.count()} valid valid records")

    # Write to local parquet file
    output_path = "/workspace/warehouse/orders_parquet"

    # Delete existing data
    import shutil
    if os.path.exists(output_path):
        shutil.rmtree(output_path)

    # Write parquet
    final_df.write.mode("overwrite").parquet(output_path)

    print(f"Successfully wrote {final_df.count()} records to parquet: {output_path}")

    # Show sample data
    print("\nSample data written:")
    spark.read.parquet(output_path).show()

    # List files
    print("\nFiles created:")
    for root, dirs, files in os.walk(output_path):
        for file in files:
            print(os.path.join(root, file))
else:
    print("No messages found in Kafka topic")

print("\nBatch job completed!")
spark.stop()

