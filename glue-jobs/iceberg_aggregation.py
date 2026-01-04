import sys
import os
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, date_trunc, count, sum, avg, countDistinct
)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'iceberg_warehouse',
    'database_name'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Iceberg with S3-compatible storage (MinIO)
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

# Use S3A catalog for MinIO (no AWS credentials needed)
spark.conf.set("spark.sql.catalog.s3a", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.s3a.warehouse", args['iceberg_warehouse'])
spark.conf.set("spark.sql.catalog.s3a.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.s3a.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# MinIO/S3 Configuration (without real AWS credentials for local)
s3_endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000")
s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "admin123")

spark.conf.set("spark.sql.catalog.s3a.s3.endpoint", s3_endpoint)
spark.conf.set("spark.sql.catalog.s3a.s3.access.key", s3_access_key)
spark.conf.set("spark.sql.catalog.s3a.s3.secret.key", s3_secret_key)
spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")

database = args['database_name']

# Read from Iceberg tables
orders_df = spark.table(f"s3a.{database}.orders")

# Daily aggregations
daily_stats = orders_df.groupBy(
    date_trunc("day", col("order_date")).alias("date"),
    col("status")
).agg(
    count("*").alias("order_count"),
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_order_value"),
    countDistinct("customer_id").alias("unique_customers")
)

# Create aggregated table
agg_table = f"s3a.{database}.daily_order_stats"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {agg_table} (
        date TIMESTAMP,
        status STRING,
        order_count BIGINT,
        total_revenue DOUBLE,
        avg_order_value DOUBLE,
        unique_customers BIGINT
    )
    USING iceberg
    PARTITIONED BY (days(date))
""")

# Write aggregated data
daily_stats.writeTo(agg_table) \
    .option("mergeSchema", "true") \
    .overwritePartitions()

print(f"Aggregated {daily_stats.count()} daily statistics")

job.commit()

