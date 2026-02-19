variable "name_prefix" {
  description = "Prefix for resource names"
  type        = string
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}

# variable "sqs_queue_arn" {
#   description = "ARN of the SQS queue for image processing notifications"
#   type        = string
#   default     = ""
# }
