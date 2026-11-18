# --------------------------------------------------------------------------- #
# Buckets: lake (raw + processed + dbt marts), Athena results, job artifacts.
# Every bucket gets the same hardening baseline.
# --------------------------------------------------------------------------- #
resource "aws_s3_bucket" "this" {
  for_each = local.buckets

  bucket        = each.value
  force_destroy = var.force_destroy_buckets
}

resource "aws_s3_bucket_ownership_controls" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each = aws_s3_bucket.this

  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

data "aws_iam_policy_document" "tls_only" {
  for_each = aws_s3_bucket.this

  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      each.value.arn,
      "${each.value.arn}/*",
    ]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id
  policy = data.aws_iam_policy_document.tls_only[each.key].json

  depends_on = [aws_s3_bucket_public_access_block.this]
}

# --------------------------------------------------------------------------- #
# Lake: versioned (protects raw history from accidental overwrite), raw JSON
# tiers down to Standard-IA, old object versions are cleaned up.
# --------------------------------------------------------------------------- #
resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.this["lake"].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.this["lake"].id

  rule {
    id     = "raw-to-infrequent-access"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    transition {
      days          = var.raw_transition_to_ia_days
      storage_class = "STANDARD_IA"
    }
  }

  rule {
    id     = "cleanup-old-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  depends_on = [aws_s3_bucket_versioning.lake]
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.this["athena_results"].id

  rule {
    id     = "expire-query-results"
    status = "Enabled"
    filter {}
    expiration {
      days = var.athena_results_retention_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.this["artifacts"].id

  rule {
    id     = "expire-glue-temp-and-logs"
    status = "Enabled"
    filter {
      prefix = "glue/tmp/"
    }
    expiration {
      days = 7
    }
  }

  rule {
    id     = "expire-spark-event-logs"
    status = "Enabled"
    filter {
      prefix = "glue/spark-logs/"
    }
    expiration {
      days = 30
    }
  }
}
