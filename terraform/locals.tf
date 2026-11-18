locals {
  name_prefix = "${var.project}-${var.environment}"
  # Glue/Athena database names can't contain hyphens. dbt uses this as DBT_SCHEMA_PREFIX.
  glue_prefix = replace(local.name_prefix, "-", "_")

  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition
  region     = data.aws_region.current.region

  # Account id suffix keeps globally-unique bucket names predictable per environment.
  buckets = {
    lake           = "${local.name_prefix}-lake-${local.account_id}"
    athena_results = "${local.name_prefix}-athena-results-${local.account_id}"
    artifacts      = "${local.name_prefix}-artifacts-${local.account_id}"
  }

  # Glue databases, one per layer. processed = Spark output; the rest are owned by dbt.
  glue_databases = {
    processed    = "Typed, flattened Parquet written by the PySpark job (source for dbt)."
    staging      = "dbt staging views: renamed and lightly cleaned source tables."
    intermediate = "dbt intermediate views: reusable joins and aggregations."
    marts        = "dbt marts: star schema (dim_* / fct_*) as Iceberg tables."
  }

  # Single source of truth for the processed-zone schema, shared with the PySpark tests.
  processed_tables = jsondecode(file("${path.module}/../contracts/processed_tables.json")).tables

  lake_bucket      = aws_s3_bucket.this["lake"].bucket
  results_bucket   = aws_s3_bucket.this["athena_results"].bucket
  artifacts_bucket = aws_s3_bucket.this["artifacts"].bucket

  tags = merge(
    {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
      Repository  = "statsbomb-elt-pipeline"
    },
    var.extra_tags,
  )
}
