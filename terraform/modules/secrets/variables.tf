variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}

variable "secrets" {
  description = "Map of secrets to store in Secrets Manager"
  type        = map(string)
  sensitive   = true
}
