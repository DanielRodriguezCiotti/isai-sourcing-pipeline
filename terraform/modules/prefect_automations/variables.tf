# Prefect Automations Module Variables

variable "environment" {
  description = "Environment name (dev, prod, etc.)"
  type        = string
}

variable "workspace_id" {
  description = "Prefect workspace ID"
  type        = string
}

# Crash Zombie Flows Configuration
variable "crash_zombie_flows_enabled" {
  description = "Whether to enable the crash zombie flows automation"
  type        = bool
  default     = true
}
