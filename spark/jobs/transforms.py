"""Funções puras reutilizáveis do pipeline de dados.

Todas as funções são DataFrame→DataFrame (ou DataFrame→resultado),
testáveis com SparkSession local, sem dependência de Kafka/Iceberg.

Módulos de cada camada (bronze_ingest, silver_purchases, gold_aggregations)
importam e compõem essas funções com a infraestrutura real.
"""

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)

# ── Constantes de domínio ───────────────────────────────────────────────────
VALID_CURRENCIES = ["BRL", "USD", "EUR"]
TOP_PRODUCTS_LIMIT = 20

# ── Constantes de qualidade ─────────────────────────────────────────────────
MIN_CURRENCY_COMPLETENESS = 0.99
FRESHNESS_DAYS = 1
GROSS_AMOUNT_TOLERANCE = 0.01


# ── Bronze → Silver ────────────────────────────────────────────────────────


def clean_purchases(bronze: DataFrame) -> DataFrame:
    """Aplica tipagem, deduplicação e regras de negócio para a camada Silver.

    Regras:
      * dedup por event_id (mantém a ingestão mais recente)
      * quantity > 0 e unit_price > 0   (regra de negócio do contrato)
      * currency dentro do domínio permitido
      * descarta linhas com chaves nulas
      * deriva gross_amount = quantity * unit_price
    """
    w = Window.partitionBy("event_id").orderBy(F.col("ingest_ts").desc())

    deduped = bronze.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    return deduped.filter(
        F.col("event_id").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("product_id").isNotNull()
        & (F.col("quantity") > 0)
        & (F.col("price") > 0)
        & F.col("currency").isin(VALID_CURRENCIES)
    ).select(
        "event_id",
        "user_id",
        "product_id",
        "quantity",
        F.col("price").alias("unit_price"),
        (F.col("quantity") * F.col("price")).alias("gross_amount"),
        "currency",
        "event_ts",
        "channel",
        F.to_date("event_ts").alias("event_date"),
    )


def revenue_windows(df: DataFrame, window_duration: str = "1 minute") -> DataFrame:
    """Agrega receita por janela de event-time (tumbling) e moeda.

    Espera um DataFrame com colunas: event_id, event_ts, currency, quantity,
    gross_amount. Em streaming, aplique ``withWatermark("event_ts", ...)`` ANTES
    de chamar esta função; em batch/teste ela funciona sobre um DF estático.

    Retorna: window_start, window_end, currency, orders, items_sold, revenue,
    avg_ticket — pronto para a tabela gold.revenue_per_minute.
    """
    return (
        df.groupBy(F.window("event_ts", window_duration), "currency")
        .agg(
            F.count("event_id").alias("orders"),
            F.sum("quantity").alias("items_sold"),
            F.round(F.sum("gross_amount"), 2).alias("revenue"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "currency",
            "orders",
            "items_sold",
            "revenue",
            F.round(F.col("revenue") / F.col("orders"), 2).alias("avg_ticket"),
        )
    )


# ── Silver → Gold ──────────────────────────────────────────────────────────


def compute_daily_revenue(silver: DataFrame) -> DataFrame:
    """Agrega receita diária por moeda a partir da Silver."""
    return (
        silver.groupBy("event_date", "currency")
        .agg(
            F.countDistinct("event_id").alias("orders"),
            F.sum("quantity").alias("items_sold"),
            F.round(F.sum("gross_amount"), 2).alias("revenue"),
        )
        .withColumn("avg_ticket", F.round(F.col("revenue") / F.col("orders"), 2))
    )


def compute_top_products(
    silver: DataFrame, limit: int = TOP_PRODUCTS_LIMIT
) -> DataFrame:
    """Ranking de produtos por receita diária (top N por dia)."""
    by_product = silver.groupBy("event_date", "product_id").agg(
        F.sum("quantity").alias("items_sold"),
        F.round(F.sum("gross_amount"), 2).alias("revenue"),
    )
    w = Window.partitionBy("event_date").orderBy(F.col("revenue").desc())
    return by_product.withColumn("rank", F.row_number().over(w)).filter(F.col("rank") <= limit)


# ── Quality checks ─────────────────────────────────────────────────────────


def run_quality_checks(df: DataFrame) -> tuple[dict[str, tuple[bool, str, bool]], int]:
    """Executa os checks de qualidade sobre a camada Silver.

    Retorna ``(checks, total)`` onde *checks* é um dict
    ``{nome: (ok, detalhe, bloqueante)}`` para cada pilar avaliado.
    """
    total = df.count()
    checks: dict[str, tuple[bool, str, bool]] = {}

    if total == 0:
        checks["non_empty"] = (False, "Silver vazia", True)
        return checks, total

    null_keys = df.filter(
        F.col("event_id").isNull() | F.col("user_id").isNull() | F.col("product_id").isNull()
    ).count()
    checks["no_null_keys"] = (null_keys == 0, f"{null_keys} linhas com chave nula", True)

    distinct_ids = df.select("event_id").distinct().count()
    checks["unique_event_id"] = (
        distinct_ids == total,
        f"{total - distinct_ids} duplicatas de event_id",
        True,
    )

    bad_price = df.filter(F.col("unit_price") <= 0).count()
    checks["price_positive"] = (bad_price == 0, f"{bad_price} linhas com unit_price <= 0", True)

    bad_qty = df.filter(F.col("quantity") <= 0).count()
    checks["quantity_positive"] = (bad_qty == 0, f"{bad_qty} linhas com quantity <= 0", True)

    bad_amount = df.filter(
        F.abs(F.col("gross_amount") - F.col("quantity") * F.col("unit_price"))
        > GROSS_AMOUNT_TOLERANCE
    ).count()
    checks["gross_amount_consistent"] = (
        bad_amount == 0,
        f"{bad_amount} linhas com gross_amount incoerente",
        True,
    )

    filled = df.filter(F.col("currency").isNotNull()).count()
    rate = filled / total
    checks["currency_completeness"] = (
        rate >= MIN_CURRENCY_COMPLETENESS,
        f"completeness={rate:.4f} (min {MIN_CURRENCY_COMPLETENESS})",
        True,
    )

    fresh = df.filter(
        F.col("event_date") >= F.date_sub(F.current_date(), FRESHNESS_DAYS)
    ).count()
    detail = f"{fresh} linhas nas últimas 24h" if fresh > 0 else "sem dados nas últimas 24h"
    checks["freshness"] = (fresh > 0, detail, False)

    return checks, total


# ── Bronze batch writing ────────────────────────────────────────────────────


def parsed_columns(schema: StructType) -> list:
    """Gera expressões de seleção para extrair campos de uma coluna ``parsed``.

    O campo ``timestamp`` é convertido para ``event_ts`` via ``to_timestamp``;
    os demais são extraídos diretamente.
    """
    cols = []
    for field in schema.fields:
        if field.name == "timestamp":
            cols.append(F.to_timestamp(f"parsed.{field.name}").alias("event_ts"))
        else:
            cols.append(F.col(f"parsed.{field.name}").alias(field.name))
    return cols


def write_bronze_batch(
    batch_df,
    batch_id: int,
    *,
    table: str,
    dlq_table: str,
    required_parsed_fields: list[str],
    schema: StructType,
) -> None:
    """Escreve um micro-batch Bronze com DLQ routing, normalização e MERGE.

    Padrão reutilizado por ``bronze_ingest`` (purchases) e ``bronze_carts``:
      1. Registros que falharam no parse ou sem campos obrigatórios → DLQ.
      2. Registros válidos → extrai colunas do parsed, dedup intra-batch, MERGE.
    """
    log = logging.getLogger(table.split(".")[-1])
    session = batch_df.sparkSession
    batch_df = batch_df.persist()
    now = F.current_timestamp()

    # ── DLQ: não parseou OU faltam campos obrigatórios mínimos ─────────────
    bad_condition = F.col("parsed").isNull()
    for field in required_parsed_fields:
        bad_condition = bad_condition | F.col(f"parsed.{field}").isNull()

    bad = batch_df.filter(bad_condition).select(
        F.col("raw_value"),
        F.lit("parse_or_required_field_violation").alias("error_reason"),
        now.alias("ingest_ts"),
        F.current_date().alias("ingest_date"),
    )
    if not bad.isEmpty():
        bad.writeTo(dlq_table).append()

    # ── Bronze válido: normaliza colunas + dedup intra-batch por event_id ───
    select_cols = parsed_columns(schema)
    select_cols.append(now.alias("ingest_ts"))

    good = (
        batch_df.filter(F.col("parsed.event_id").isNotNull())
        .select(*select_cols)
        .withColumn("event_date", F.to_date("event_ts"))
        .dropDuplicates(["event_id"])
    )

    view_name = f"{table.split('.')[-1]}_updates"
    good.createOrReplaceTempView(view_name)
    session.sql(f"""
        MERGE INTO {table} t
        USING {view_name} s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    batch_df.unpersist()
    log.info("batch %d processado.", batch_id)
