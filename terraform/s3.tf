resource "aws_s3_bucket" "iceberg_data_lake" {
  bucket = "${var.project_name}-iceberg-${var.environment}"
}

resource "aws_s3_bucket_versioning" "iceberg_versioning" {
  bucket = aws_s3_bucket.iceberg_data_lake.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "iceberg_encryption" {
  bucket = aws_s3_bucket.iceberg_data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# S3 bucket for Glue scripts
resource "aws_s3_bucket" "glue_scripts" {
  bucket = "${var.project_name}-glue-scripts-${var.environment}"
}

# S3 bucket for Glue temporary files
resource "aws_s3_bucket" "glue_temp" {
  bucket = "${var.project_name}-glue-temp-${var.environment}"
}

# Upload Glue scripts
resource "aws_s3_object" "glue_job_scripts" {
  for_each = fileset("${path.module}/../glue-jobs/", "*.py")
  
  bucket = aws_s3_bucket.glue_scripts.id
  key    = each.value
  source = "${path.module}/../glue-jobs/${each.value}"
  etag   = filemd5("${path.module}/../glue-jobs/${each.value}")
}