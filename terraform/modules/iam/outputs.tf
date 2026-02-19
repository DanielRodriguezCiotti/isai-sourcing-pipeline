output "task_execution_role_arn" {
  description = "ARN of the task execution role"
  value       = aws_iam_role.task_execution.arn
}

output "task_role_arn" {
  description = "ARN of the task role"
  value       = aws_iam_role.task.arn
}

output "prefect_ecs_user_access_key_id" {
  description = "Access key ID for the Prefect ECS user"
  value       = aws_iam_access_key.prefect_ecs.id
}

output "prefect_ecs_user_secret_access_key" {
  description = "Secret access key for the Prefect ECS user"
  value       = aws_iam_access_key.prefect_ecs.secret
  sensitive   = true
}

output "prefect_ecs_user_name" {
  description = "Name of the Prefect ECS user"
  value       = aws_iam_user.prefect_ecs.name
}
