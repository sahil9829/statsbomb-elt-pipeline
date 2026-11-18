variable "project" {
  description = "Short project name used as a prefix for every resource."
  type        = string
  default     = "statsbomb"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,20}$", var.project))
    error_message = "project must be lowercase alphanumeric/hyphens, 2-21 chars."
  }
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
  type        = string

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of dev, staging, prod."
  }
}

variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "eu-west-2"
}

variable "glue_worker_type" {
  description = "Glue worker type for the Spark job."
  type        = string
  default     = "G.1X"

  validation {
    condition     = contains(["G.025X", "G.1X", "G.2X", "G.4X", "G.8X"], var.glue_worker_type)
    error_message = "Unsupported Glue worker type."
  }
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers (minimum 2)."
  type        = number
  default     = 2

  validation {
    condition     = var.glue_number_of_workers >= 2
    error_message = "Glue jobs need at least 2 workers."
  }
}

variable "glue_timeout_minutes" {
  description = "Hard timeout for a Glue job run."
  type        = number
  default     = 60
}

variable "athena_bytes_scanned_cutoff" {
  description = "Per-query scan limit for the Athena workgroup (cost guard rail). Minimum 10 MB."
  type        = number
  default     = 10737418240 # 10 GiB
}

variable "athena_results_retention_days" {
  description = "Days to keep Athena query results before expiring them."
  type        = number
  default     = 7
}

variable "raw_transition_to_ia_days" {
  description = "Days before raw JSON moves to S3 Standard-IA."
  type        = number
  default     = 30
}

variable "log_retention_days" {
  description = "CloudWatch log retention for the Glue job."
  type        = number
  default     = 30
}

variable "pipeline_trusted_principal_arns" {
  description = <<-EOT
    IAM principals allowed to assume the pipeline role (e.g. the role your Airflow
    workers run as). Empty = any principal in this account with sts:AssumeRole permission.
  EOT
  type        = list(string)
  default     = []
}

variable "force_destroy_buckets" {
  description = "Allow terraform destroy to delete non-empty buckets. Keep false outside dev."
  type        = bool
  default     = false
}

variable "extra_tags" {
  description = "Additional tags applied to every resource."
  type        = map(string)
  default     = {}
}
