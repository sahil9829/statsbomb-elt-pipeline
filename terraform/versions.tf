terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Partial configuration - supply bucket/key/region with
  #   terraform init -backend-config=environments/dev.s3.tfbackend
  backend "s3" {}
}
