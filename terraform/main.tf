# ─────────────────────────────────────────────────────────────────────────────
# IaC (stub) — provisiona o equivalente de produção do lakehouse na AWS.
# NÃO é aplicado pelo slice local (que usa MinIO + Iceberg REST). Serve para
# demonstrar o caminho local -> cloud SEM mudar o código dos jobs Spark.
# `terraform init && terraform plan` valida; `apply` exige credenciais reais.
# ─────────────────────────────────────────────────────────────────────────────
terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region = var.region
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "project" {
  type    = string
  default = "ecommerce-lakehouse"
}

# Warehouse do lakehouse (equivalente ao bucket MinIO `warehouse`).
resource "aws_s3_bucket" "warehouse" {
  bucket = "${var.project}-warehouse"
}

resource "aws_s3_bucket_lifecycle_configuration" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id

  # Bronze é barato e histórico: move para storage frio depois de 90 dias.
  rule {
    id     = "bronze-tiering"
    status = "Enabled"
    filter { prefix = "bronze/" }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }
}

# Catálogo Iceberg gerenciado (equivalente ao Iceberg REST local).
resource "aws_glue_catalog_database" "bronze" { name = "${var.project}_bronze" }
resource "aws_glue_catalog_database" "silver" { name = "${var.project}_silver" }
resource "aws_glue_catalog_database" "gold" { name = "${var.project}_gold" }

output "warehouse_bucket" {
  value = aws_s3_bucket.warehouse.bucket
}
