# --------------------------------------------------------------------------- #
# Managed Spark: the exact PySpark job from spark/jobs, run on AWS Glue 5.0
# (Spark 3.5 / Python 3.11). Airflow triggers it when SPARK_RUNNER=glue.
# --------------------------------------------------------------------------- #
resource "aws_s3_object" "spark_job" {
  bucket = local.artifacts_bucket
  key    = "glue/scripts/statsbomb_transform.py"
  source = "${path.module}/../spark/jobs/statsbomb_transform.py"
  etag   = filemd5("${path.module}/../spark/jobs/statsbomb_transform.py")
}

resource "aws_cloudwatch_log_group" "glue_transform" {
  name              = "/aws-glue/jobs/${local.name_prefix}-statsbomb-transform"
  retention_in_days = var.log_retention_days
}

resource "aws_glue_job" "transform" {
  name              = "${local.name_prefix}-statsbomb-transform"
  description       = "Raw StatsBomb JSON -> processed Parquet (spark/jobs/statsbomb_transform.py)"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "5.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = var.glue_timeout_minutes
  max_retries       = 0 # retries are owned by Airflow
  execution_class   = "STANDARD"

  command {
    name            = "glueetl"
    script_location = "s3://${local.artifacts_bucket}/${aws_s3_object.spark_job.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--lake-root"                        = "s3://${local.lake_bucket}"
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable" # full refresh by design
    "--TempDir"                          = "s3://${local.artifacts_bucket}/glue/tmp/"
    "--enable-metrics"                   = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_transform.name
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${local.artifacts_bucket}/glue/spark-logs/"
  }

  execution_property {
    max_concurrent_runs = 1 # full-refresh overwrite must never run concurrently
  }
}
