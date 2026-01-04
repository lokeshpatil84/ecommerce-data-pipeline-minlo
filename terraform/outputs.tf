output "iceberg_bucket" {
  value = aws_s3_bucket.iceberg_data_lake.bucket
}

output "glue_database" {
  value = aws_glue_catalog_database.ecommerce.name
}

output "vpc_id" {
  value = aws_vpc.main.id
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.main.name
}