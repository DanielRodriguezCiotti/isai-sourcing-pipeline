# General variables
environment  = "dev"
project_name = "isai-etl"
aws_region   = "eu-north-1"

# Network variables
vpc_id              = "vpc-000302e4328136292"
azs                 = ["eu-north-1a", "eu-north-1b"]
public_subnet_cidrs = ["172.31.64.0/20", "172.31.80.0/20"]

work_pool = {
  pool_name   = "ecs-push-pool"
  pool_cpu    = 1024
  pool_memory = 2048
}

task_env_vars = [
  {
    name  = "ENV"
    value = "dev"
  }
]