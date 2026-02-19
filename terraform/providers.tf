terraform {
  backend "s3" {
    bucket = "isai-sourcing-tf-state"
    key    = "terraform.tfstate"
    region = "eu-north-1"
  }

  required_providers {
    prefect = {
      source  = "prefecthq/prefect"
      version = "2.28.0"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.3.0"
    }
  }
}

# Configure the AWS Provider
# Credentials are obtained via OIDC in GitHub Actions or from AWS CLI/environment locally
provider "aws" {
  region = var.aws_region
}

# Configure the Prefect Provider
provider "prefect" {
  account_id   = var.prefect_config.organization_id
  workspace_id = var.prefect_config.workspace_id
  api_key      = var.prefect_config.api_key
}
