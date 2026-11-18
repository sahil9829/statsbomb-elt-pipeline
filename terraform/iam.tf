locals {
  lake_arn      = aws_s3_bucket.this["lake"].arn
  results_arn   = aws_s3_bucket.this["athena_results"].arn
  artifacts_arn = aws_s3_bucket.this["artifacts"].arn

  glue_catalog_arns = [
    "arn:${local.partition}:glue:${local.region}:${local.account_id}:catalog",
    "arn:${local.partition}:glue:${local.region}:${local.account_id}:database/${local.glue_prefix}_*",
    "arn:${local.partition}:glue:${local.region}:${local.account_id}:table/${local.glue_prefix}_*/*",
  ]
}

# =========================================================================== #
# Glue job role - least privilege: read raw/, write processed/, nothing else.
# =========================================================================== #
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "glue_job" {
  name               = "${local.name_prefix}-glue-transform"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

data "aws_iam_policy_document" "glue_job" {
  statement {
    sid       = "ListBuckets"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [local.lake_arn, local.artifacts_arn]
  }

  statement {
    sid       = "ReadRaw"
    actions   = ["s3:GetObject"]
    resources = ["${local.lake_arn}/raw/*"]
  }

  statement {
    sid       = "WriteProcessed"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${local.lake_arn}/processed/*"]
  }

  statement {
    sid       = "ReadScript"
    actions   = ["s3:GetObject"]
    resources = ["${local.artifacts_arn}/glue/scripts/*"]
  }

  statement {
    sid       = "GlueScratchAndSparkUi"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${local.artifacts_arn}/glue/tmp/*", "${local.artifacts_arn}/glue/spark-logs/*"]
  }

  statement {
    sid     = "Logs"
    actions = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:AssociateKmsKey"]
    resources = [
      "${aws_cloudwatch_log_group.glue_transform.arn}:*",
      "arn:${local.partition}:logs:${local.region}:${local.account_id}:log-group:/aws-glue/*",
    ]
  }

  statement {
    sid       = "Metrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["Glue"]
    }
  }
}

resource "aws_iam_role_policy" "glue_job" {
  name   = "statsbomb-transform"
  role   = aws_iam_role.glue_job.id
  policy = data.aws_iam_policy_document.glue_job.json
}

# =========================================================================== #
# Pipeline role - assumed by Airflow to extract, trigger Glue and run dbt on
# Athena. The permissions are a standalone managed policy so they can also be
# attached to an existing user/role (e.g. an MWAA execution role).
# Terraform never creates access keys: no long-lived secrets in state.
# =========================================================================== #
data "aws_iam_policy_document" "pipeline_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type = "AWS"
      identifiers = (
        length(var.pipeline_trusted_principal_arns) > 0
        ? var.pipeline_trusted_principal_arns
        : ["arn:${local.partition}:iam::${local.account_id}:root"]
      )
    }
  }
}

resource "aws_iam_role" "pipeline" {
  name                 = "${local.name_prefix}-pipeline"
  assume_role_policy   = data.aws_iam_policy_document.pipeline_assume.json
  max_session_duration = 3600 * 4
}

data "aws_iam_policy_document" "pipeline" {
  statement {
    sid       = "ListBuckets"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [local.lake_arn, local.results_arn]
  }

  statement {
    sid     = "LakeReadWrite"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = [
      "${local.lake_arn}/raw/*",       # extract
      "${local.lake_arn}/processed/*", # local Spark runner
      "${local.lake_arn}/dbt/*",       # dbt-athena table data
    ]
  }

  statement {
    sid       = "AthenaResults"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${local.results_arn}/*"]
  }

  statement {
    sid = "Athena"
    actions = [
      "athena:StartQueryExecution",
      "athena:StopQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetQueryRuntimeStatistics",
      "athena:GetWorkGroup",
      "athena:BatchGetQueryExecution",
      "athena:ListQueryExecutions",
    ]
    resources = [aws_athena_workgroup.this.arn]
  }

  statement {
    sid       = "AthenaCatalog"
    actions   = ["athena:GetDataCatalog", "athena:ListDataCatalogs", "athena:ListWorkGroups"]
    resources = ["*"]
  }

  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetTableVersions",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:BatchDeleteTable",
      "glue:DeleteTableVersion",
      "glue:BatchDeleteTableVersion",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchCreatePartition",
      "glue:BatchDeletePartition",
    ]
    resources = local.glue_catalog_arns
  }

  statement {
    sid       = "RunGlueJob"
    actions   = ["glue:GetJob", "glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
    resources = [aws_glue_job.transform.arn]
  }

  statement {
    sid       = "ReadGlueLogs"
    actions   = ["logs:GetLogEvents", "logs:FilterLogEvents", "logs:DescribeLogStreams"]
    resources = ["${aws_cloudwatch_log_group.glue_transform.arn}:*"]
  }
}

resource "aws_iam_policy" "pipeline" {
  name        = "${local.name_prefix}-pipeline"
  description = "Run the StatsBomb ELT pipeline: S3 lake, Glue job, Glue Catalog, Athena."
  policy      = data.aws_iam_policy_document.pipeline.json
}

resource "aws_iam_role_policy_attachment" "pipeline" {
  role       = aws_iam_role.pipeline.name
  policy_arn = aws_iam_policy.pipeline.arn
}
