# Environment and basic configuration
variable "environment" {
  description = "Environment name (dev, prod, etc.)"
  type        = string
}

variable "workspace_id" {
  description = "Prefect workspace ID"
  type        = string
}

# Work pool names
variable "pool_name" {
  description = "Name for the work pool"
  type        = string
}

variable "pool_cpu" {
  description = "CPU to allocate to work pool"
  type = string
}

variable "pool_memory" {
  description = "Memory (MiB) to allocate to the pool ECS tasks"
  type        = number
  default     = 2048
}

variable "image" {
  description = "Docker image for ECS tasks"
  type        = string
}

variable "envvars" {
  description = "Environment variables for ECS tasks"
  type        = map(string)
  default     = {}
}

variable "vpc_id" {
  description = "VPC ID for ECS tasks"
  type        = string
}

variable "cluster" {
  description = "ECS cluster name"
  type        = string
}

variable "command" {
  description = "Command to run in ECS tasks"
  type        = string
  default     = "uv run python -m prefect.engine"
}

variable "task_role_arn" {
  description = "IAM role ARN for ECS tasks"
  type        = string
}

variable "container_name" {
  description = "Container name for ECS tasks"
  type        = string
  default     = "prefect"
}

variable "execution_role_arn" {
  description = "IAM execution role ARN for ECS tasks"
  type        = string
}

variable "subnets" {
  description = "Subnet IDs for ECS tasks"
  type        = list(string)
}

variable "security_groups" {
  description = "Security group IDs for ECS tasks"
  type        = list(string)
}

# AWS credentials for Prefect block
variable "aws_access_key_id" {
  description = "AWS access key ID"
  type        = string
  sensitive   = true
}

variable "aws_secret_access_key" {
  description = "AWS secret access key"
  type        = string
  sensitive   = true
}

variable "region_name" {
  description = "AWS region name"
  type        = string
}

# ECS task configuration
variable "secrets_arn" {
  description = "ARN of AWS Secrets Manager secret"
  type        = string
  default     = ""
}

