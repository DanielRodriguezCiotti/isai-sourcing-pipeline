

output "task_execution_role_arn" {
  description = "ARN of the task execution role"
  value       = module.iam.task_execution_role_arn
}

output "task_role_arn" {
  description = "ARN of the task role"
  value       = module.iam.task_role_arn
}

output "ecr_repository_url" {
  description = "URL of the ECR repository"
  value       = module.ecr.repository_url
}

output "ecr_repository_name" {
  description = "Name of the ECR repository"
  value       = module.ecr.repository_name
}

output "public_subnet_ids" {
  description = "IDs of the public subnets"
  value       = module.networking.public_subnet_ids
}

output "security_group_id" {
  description = "ID of the ECS security group"
  value       = module.networking.security_group_id
}


output "ecr_repository_arn" {
  description = "ARN of the ECR repository"
  value       = module.ecr.repository_arn
}

output "secret_arn" {
  description = "ARN of the secret"
  value       = module.secrets.secret_arn
}

output "secret_name" {
  description = "Name of the secret"
  value       = module.secrets.secret_name
}

output "prefect_automations_names" {
  description = "Names of the prefect automations"
  value       = module.prefect_automations.automation_names
}