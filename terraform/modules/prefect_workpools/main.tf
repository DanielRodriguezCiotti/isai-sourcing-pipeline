# Prefect Work Pools Module
# Manages (ecs:push) work pool


# AWS credentials block for ECS work pool
resource "prefect_block" "aws_credentials" {
  name      = "${var.environment}-aws-credentials"
  type_slug = "aws-credentials"
  data = jsonencode({
    aws_access_key_id     = var.aws_access_key_id
    aws_secret_access_key = var.aws_secret_access_key
    region_name           = var.region_name
  })
}

# Slow work pool (ecs:push type) for compute-intensive tasks
resource "prefect_work_pool" "ecs_work_pool" {
  name         = var.pool_name
  type         = "ecs:push"
  workspace_id = var.workspace_id
  paused       = false
  base_job_template = templatefile("${path.module}/ecs-base-job-template.json.tpl", {
    image              = var.image
    envvars            = jsonencode(var.envvars)
    secrets_arn        = var.secrets_arn
    vpc_id             = var.vpc_id
    cluster            = var.cluster
    command            = var.command
    task_role_arn      = var.task_role_arn
    container_name     = var.container_name
    block_document_id  = prefect_block.aws_credentials.id
    execution_role_arn = var.execution_role_arn
    subnets            = var.subnets
    security_groups    = var.security_groups
    pool_memory   = var.pool_memory
    pool_cpu = var.pool_cpu
  })
}
