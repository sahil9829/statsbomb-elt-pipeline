output "lake_bucket" {
  description = "Data lake bucket (raw/, processed/, dbt/)."
  value       = local.lake_bucket
}

output "athena_results_bucket" {
  value = local.results_bucket
}

output "artifacts_bucket" {
  value = local.artifacts_bucket
}

output "athena_workgroup" {
  value = aws_athena_workgroup.this.name
}

output "glue_job_name" {
  value = aws_glue_job.transform.name
}

output "glue_databases" {
  value = { for k, db in aws_glue_catalog_database.this : k => db.name }
}

output "pipeline_role_arn" {
  description = "Role for Airflow to assume."
  value       = aws_iam_role.pipeline.arn
}

output "pipeline_policy_arn" {
  description = "Attach this to an existing principal instead of assuming the role, if preferred."
  value       = aws_iam_policy.pipeline.arn
}

output "dotenv" {
  description = "Non-secret settings to paste into the project's .env for SPARK_RUNNER=glue / DBT_TARGET=athena."
  value       = <<-EOT
    AWS_DEFAULT_REGION=${local.region}
    S3_BUCKET_NAME=${local.lake_bucket}
    LAKE_ROOT=s3://${local.lake_bucket}
    SPARK_RUNNER=glue
    GLUE_JOB_NAME=${aws_glue_job.transform.name}
    DBT_TARGET=athena
    DBT_SCHEMA_PREFIX=${local.glue_prefix}
    ATHENA_WORKGROUP=${aws_athena_workgroup.this.name}
    ATHENA_S3_STAGING_DIR=s3://${local.results_bucket}/query-results/
    ATHENA_S3_DATA_DIR=s3://${local.lake_bucket}/dbt/
  EOT
}
