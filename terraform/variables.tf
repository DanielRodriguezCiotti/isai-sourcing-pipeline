# General variables

variable "environment" {
  description = "Environment name (e.g., dev, prod)"
  type        = string
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

# AWS auth - Using OIDC via GitHub Actions (no static credentials needed)

# Network variables

variable "vpc_id" {
  description = "Existing VPC ID"
  type        = string
}

variable "azs" {
  description = "Availability zones"
  type        = list(string)
  default     = ["eu-north-1a", "eu-north-1b"]
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}


# Secrets variables

variable "llm_api_key" {
  description = "LLM API Key"
  type        = string
  sensitive   = true
}

variable "prefect_config" {
  description = "Prefect Cloud configuration"
  type = object({
    api_key         = string
    organization_id = string
    workspace_id    = string
  })
  sensitive = true
}

variable "work_pool" {
  description = "Work pool configuration"
  type = object({
    pool_name = string
    pool_cpu  = number
    pool_memory = number
  })
}

variable "task_env_vars" {
  description = "Environment variables for Pool"
  type = list(object({
    name  = string
    value = string
  }))
}

