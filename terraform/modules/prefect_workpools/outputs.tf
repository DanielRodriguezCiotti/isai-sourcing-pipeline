# Work pool outputs
output "fast_pool_name" {
  description = "Name of the fast work pool"
  value       = prefect_work_pool.fast_pool.name
}

output "fast_pool_id" {
  description = "ID of the fast work pool"
  value       = prefect_work_pool.fast_pool.id
}

output "slow_pool_name" {
  description = "Name of the slow work pool"
  value       = prefect_work_pool.ecs_work_pool.name
}

output "slow_pool_id" {
  description = "ID of the slow work pool"
  value       = prefect_work_pool.ecs_work_pool.id
}

# AWS credentials block
output "aws_credentials_block_id" {
  description = "ID of the AWS credentials block"
  value       = prefect_block.aws_credentials.id
}
