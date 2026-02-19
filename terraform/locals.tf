locals {
  name_prefix = "${var.environment}-${var.project_name}"
  common_tags = {
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "terraform"
  }

  prefect_api_url                 = "https://api.prefect.cloud/api/accounts/${var.prefect_config.organization_id}/workspaces/${var.prefect_config.workspace_id}"
  prefect_ecs_task_command        = "uv run python -m prefect.engine"
  prefect_ecs_task_container_name = "prefect"
}