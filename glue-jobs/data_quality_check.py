
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'iceberg_warehouse', 'database_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Iceberg
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", args['iceberg_warehouse'])

database = args['database_name']
orders_df = spark.table(f"glue_catalog.{database}.orders")

# Data Quality Checks
quality_checks = {
    "total_records": orders_df.count(),
    "null_order_ids": orders_df.filter(col("order_id").isNull()).count(),
    "negative_amounts": orders_df.filter(col("total_amount") < 0).count(),
    "future_dates": orders_df.filter(col("order_date") > current_timestamp()).count(),
    "duplicate_orders": orders_df.groupBy("order_id").count().filter(col("count") > 1).count()
}

print("Data Quality Report:")
for check, value in quality_checks.items():
    print(f"{check}: {value}")
    if value > 0 and check != "total_records":
        print(f"WARNING: Data quality issue detected in {check}")

job.commit()