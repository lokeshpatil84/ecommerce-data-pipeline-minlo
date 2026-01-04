resource "aws_glue_catalog_database" "ecommerce" {
  name = "${var.project_name}_${var.environment}"
}

# Glue Connection for Kafka
resource "aws_glue_connection" "kafka" {
  name = "${var.project_name}-kafka-connection"

  connection_properties = {
    KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
    KAFKA_SSL_ENABLED       = "false"
  }

  physical_connection_requirements {
    availability_zone      = data.aws_availability_zones.available.names[0]
    security_group_id_list = [aws_security_group.glue.id]
    subnet_id              = aws_subnet.private[0].id
  }
}

# Glue Job - Kafka to Iceberg
resource "aws_glue_job" "kafka_to_iceberg" {
  name     = "${var.project_name}-kafka-to-iceberg"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/kafka_to_iceberg.py"
    python_version  = "3"
  }

  default_arguments = {
    "--TempDir"                          = "s3://${aws_s3_bucket.glue_temp.bucket}/temp/"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.glue_temp.bucket}/spark-logs/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--kafka_bootstrap_servers"          = "kafka:9092"
    "--kafka_topic"                      = "dbserver1.ecommerce.orders"
    "--iceberg_warehouse"                = "s3://${aws_s3_bucket.iceberg_data_lake.bucket}/warehouse"
    "--database_name"                    = aws_glue_catalog_database.ecommerce.name
    "--table_name"                       = "orders"
    "--datalake-formats"                 = "iceberg"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  execution_property {
    max_concurrent_runs = 1
  }
}

# Glue Job - Iceberg Aggregation
resource "aws_glue_job" "iceberg_aggregation" {
  name     = "${var.project_name}-iceberg-aggregation"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/iceberg_aggregation.py"
    python_version  = "3"
  }

  default_arguments = {
    "--TempDir"                          = "s3://${aws_s3_bucket.glue_temp.bucket}/temp/"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.glue_temp.bucket}/spark-logs/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--iceberg_warehouse"                = "s3://${aws_s3_bucket.iceberg_data_lake.bucket}/warehouse"
    "--database_name"                    = aws_glue_catalog_database.ecommerce.name
    "--datalake-formats"                 = "iceberg"
    "--spark.sql.catalog.sparkstore.committer-algorithm-version" = "2"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60
}

