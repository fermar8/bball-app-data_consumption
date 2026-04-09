variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "eu-west-3"
}

variable "bootstrap_state_bucket" {
  description = "Name of the S3 bucket containing bootstrap Terraform state"
  type        = string
  default     = "tfstate-590183661886-eu-west-3"
}

variable "function_name" {
  description = "Name of the Lambda function (without environment suffix)"
  type        = string
  default     = "data-consumption-teams-static"
}

variable "environment" {
  description = "Deployment environment (live or nonlive)"
  type        = string

  validation {
    condition     = contains(["live", "nonlive"], var.environment)
    error_message = "Environment must be either 'live' or 'nonlive'."
  }
}

variable "project_name" {
  description = "Name of the project (used for resource naming)"
  type        = string
  default     = "bball-app"
}

variable "nba_data_bucket_name" {
  description = "Name of the S3 bucket containing raw NBA API data"
  type        = string
  default     = "bball-app-nba-data"
}

variable "timeout" {
  description = "Lambda function timeout in seconds"
  type        = number
  default     = 60
}

variable "memory_size" {
  description = "Lambda function memory size in MB"
  type        = number
  default     = 128
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    Project   = "bball-app"
    ManagedBy = "terraform"
  }
}

variable "alarm_emails" {
  description = "List of email addresses to receive CloudWatch alarm notifications"
  type        = list(string)
  default     = []
}
