"""Schemas Spark centralizados para os data contracts do pipeline.

Cada StructType corresponde a um contrato versionado em ``contracts/``.
Centralizar aqui evita duplicação e garante que todos os jobs parseem
eventos com a mesma definição de tipos.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── Infraestrutura compartilhada ────────────────────────────────────────────
KAFKA_BOOTSTRAP = "kafka:9092"

# ── purchase.v1 ─────────────────────────────────────────────────────────────
PURCHASE_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("user_id", LongType()),
        StructField("product_id", LongType()),
        StructField("quantity", IntegerType()),
        StructField("price", DoubleType()),
        StructField("currency", StringType()),
        StructField("timestamp", StringType()),
        StructField("channel", StringType()),
        StructField("schema_version", StringType()),
    ]
)

# ── cart.v1 (purchase + cart_id) ────────────────────────────────────────────
CART_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("cart_id", StringType()),
        StructField("user_id", LongType()),
        StructField("product_id", LongType()),
        StructField("quantity", IntegerType()),
        StructField("price", DoubleType()),
        StructField("currency", StringType()),
        StructField("timestamp", StringType()),
        StructField("channel", StringType()),
        StructField("schema_version", StringType()),
    ]
)

# ── Schema reduzido para carrinhos abandonados ──────────────────────────────
ABANDONED_CART_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("cart_id", StringType()),
        StructField("user_id", LongType()),
        StructField("quantity", IntegerType()),
        StructField("price", DoubleType()),
        StructField("timestamp", StringType()),
    ]
)
