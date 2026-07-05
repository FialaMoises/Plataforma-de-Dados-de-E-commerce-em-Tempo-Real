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

locals {
  common_tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# S3 — Warehouse (equivalente ao bucket MinIO `warehouse`)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "warehouse" {
  bucket = "${var.project}-${var.environment}-warehouse"
  tags   = local.common_tags
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

# ─────────────────────────────────────────────────────────────────────────────
# S3 — Checkpoints (Spark Structured Streaming checkpoint location)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "checkpoints" {
  bucket = "${var.project}-${var.environment}-checkpoints"
  tags   = local.common_tags
}

resource "aws_s3_bucket_lifecycle_configuration" "checkpoints" {
  bucket = aws_s3_bucket.checkpoints.id

  # Checkpoint data older than 30 days is unlikely to be needed.
  rule {
    id     = "expire-old-checkpoints"
    status = "Enabled"
    filter {}
    expiration {
      days = 30
    }
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# S3 — Dead-Letter Queue archive (mensagens descartadas pelo pipeline)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "dlq_archive" {
  bucket = "${var.project}-${var.environment}-dlq-archive"
  tags   = local.common_tags
}

resource "aws_s3_bucket_lifecycle_configuration" "dlq_archive" {
  bucket = aws_s3_bucket.dlq_archive.id

  # Keep DLQ records for 180 days then move to deep archive.
  rule {
    id     = "dlq-deep-archive"
    status = "Enabled"
    filter {}
    transition {
      days          = 180
      storage_class = "DEEP_ARCHIVE"
    }
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# Glue Catalog — databases Iceberg (equivalente ao Iceberg REST local)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_glue_catalog_database" "bronze" {
  name = "${var.project}_${var.environment}_bronze"
  tags = local.common_tags
}

resource "aws_glue_catalog_database" "silver" {
  name = "${var.project}_${var.environment}_silver"
  tags = local.common_tags
}

resource "aws_glue_catalog_database" "gold" {
  name = "${var.project}_${var.environment}_gold"
  tags = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# IAM — EMR / Spark job role (acesso a S3 warehouse + checkpoints + DLQ)
# ─────────────────────────────────────────────────────────────────────────────
data "aws_iam_policy_document" "emr_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["elasticmapreduce.amazonaws.com", "ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_spark" {
  name               = "${var.project}-${var.environment}-emr-spark"
  assume_role_policy = data.aws_iam_policy_document.emr_assume_role.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "emr_s3_access" {
  statement {
    sid    = "S3ReadWriteLakehouse"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.warehouse.arn,
      "${aws_s3_bucket.warehouse.arn}/*",
      aws_s3_bucket.checkpoints.arn,
      "${aws_s3_bucket.checkpoints.arn}/*",
      aws_s3_bucket.dlq_archive.arn,
      "${aws_s3_bucket.dlq_archive.arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "emr_s3_access" {
  name   = "s3-lakehouse-access"
  role   = aws_iam_role.emr_spark.id
  policy = data.aws_iam_policy_document.emr_s3_access.json
}

# ─────────────────────────────────────────────────────────────────────────────
# IAM — Glue Catalog access role (para jobs que registram/leem tabelas)
# ─────────────────────────────────────────────────────────────────────────────
data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_catalog" {
  name               = "${var.project}-${var.environment}-glue-catalog"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "glue_catalog_access" {
  statement {
    sid    = "GlueCatalogFullAccess"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:CreatePartition",
      "glue:UpdatePartition",
      "glue:DeletePartition",
      "glue:BatchCreatePartition",
    ]
    resources = ["*"]
  }

  # Glue jobs also need S3 read access to the warehouse.
  statement {
    sid    = "S3ReadWarehouse"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.warehouse.arn,
      "${aws_s3_bucket.warehouse.arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_catalog_access" {
  name   = "glue-catalog-access"
  role   = aws_iam_role.glue_catalog.id
  policy = data.aws_iam_policy_document.glue_catalog_access.json
}
