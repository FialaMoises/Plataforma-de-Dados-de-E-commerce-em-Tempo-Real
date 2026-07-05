# ─────────────────────────────────────────────────────────────────────────────
# Variables — centraliza todas as variáveis do módulo de infraestrutura.
# ─────────────────────────────────────────────────────────────────────────────

variable "region" {
  description = "AWS region where resources will be provisioned."
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project slug used as prefix for resource names and tags."
  type        = string
  default     = "ecommerce-lakehouse"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,30}$", var.project))
    error_message = "project must be 3-31 lowercase alphanumeric characters or hyphens, starting with a letter."
  }
}

variable "environment" {
  description = "Deployment environment (dev, staging, or prod)."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}
