# ─────────────────────────────────────────────────────────────────────────────
# Outputs — valores exportados para uso em outros módulos ou scripts.
# ─────────────────────────────────────────────────────────────────────────────

# S3 Buckets
output "warehouse_bucket" {
  description = "Name of the S3 bucket used as the Iceberg warehouse."
  value       = aws_s3_bucket.warehouse.bucket
}

output "warehouse_bucket_arn" {
  description = "ARN of the warehouse S3 bucket."
  value       = aws_s3_bucket.warehouse.arn
}

output "checkpoints_bucket" {
  description = "Name of the S3 bucket for Spark Structured Streaming checkpoints."
  value       = aws_s3_bucket.checkpoints.bucket
}

output "checkpoints_bucket_arn" {
  description = "ARN of the checkpoints S3 bucket."
  value       = aws_s3_bucket.checkpoints.arn
}

output "dlq_archive_bucket" {
  description = "Name of the S3 bucket for dead-letter queue archives."
  value       = aws_s3_bucket.dlq_archive.bucket
}

output "dlq_archive_bucket_arn" {
  description = "ARN of the DLQ archive S3 bucket."
  value       = aws_s3_bucket.dlq_archive.arn
}

# Glue Catalog databases
output "glue_database_bronze" {
  description = "Glue catalog database name for the bronze layer."
  value       = aws_glue_catalog_database.bronze.name
}

output "glue_database_silver" {
  description = "Glue catalog database name for the silver layer."
  value       = aws_glue_catalog_database.silver.name
}

output "glue_database_gold" {
  description = "Glue catalog database name for the gold layer."
  value       = aws_glue_catalog_database.gold.name
}

# IAM Roles
output "emr_spark_role_arn" {
  description = "ARN of the IAM role for EMR/Spark jobs."
  value       = aws_iam_role.emr_spark.arn
}

output "glue_catalog_role_arn" {
  description = "ARN of the IAM role for Glue catalog access."
  value       = aws_iam_role.glue_catalog.arn
}
