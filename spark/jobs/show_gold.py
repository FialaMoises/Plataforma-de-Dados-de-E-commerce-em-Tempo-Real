"""Mostra o estado do lakehouse: contagens por camada + KPIs do Gold.

Também valida a idempotência do Bronze: nº de linhas == nº de event_id distintos
(se o MERGE estiver correto, não há duplicatas apesar dos eventos repetidos).
"""

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("show-gold").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bronze = spark.read.table("lakehouse.bronze.purchases")
b_rows = bronze.count()
b_ids = bronze.select("event_id").distinct().count()
dlq = spark.read.table("lakehouse.bronze.purchases_dlq").count()
silver = spark.read.table("lakehouse.silver.purchases").count()

idem = "OK (sem duplicatas)" if b_rows == b_ids else "FALHA: há duplicatas!"
logger.info("──────── ESTADO DO LAKEHOUSE ────────")
logger.info("  bronze.purchases       : %d linhas / %d event_id distintos", b_rows, b_ids)
logger.info("  -> idempotência Bronze  : %s", idem)
logger.info("  bronze.purchases_dlq   : %d payloads inválidos (DLQ)", dlq)
logger.info("  silver.purchases       : %d linhas", silver)
logger.info("─────────────────────────────────────\n")

logger.info("KPIs — gold.daily_revenue (top 10 dias):")
(
    spark.read.table("lakehouse.gold.daily_revenue")
    .orderBy(F.col("event_date").desc(), F.col("revenue").desc())
    .show(10, False)
)

logger.info("KPIs — gold.top_products (top 10 do dia mais recente):")
g = spark.read.table("lakehouse.gold.top_products")
latest = g.agg(F.max("event_date")).collect()[0][0]
if latest:
    g.filter(F.col("event_date") == latest).orderBy("rank").show(10, False)

logger.info("KPIs — gold.revenue_per_minute (últimas 10 janelas, Fase 2):")
(
    spark.read.table("lakehouse.gold.revenue_per_minute")
    .orderBy(F.col("window_start").desc())
    .show(10, False)
)

logger.info("KPIs — gold.abandoned_carts (resumo, Fase 2 stateful):")
(
    spark.read.table("lakehouse.gold.abandoned_carts")
    .agg(
        F.count("*").alias("abandonados"),
        F.round(F.sum("cart_value"), 2).alias("valor_perdido"),
        F.round(F.avg("items"), 2).alias("itens_medios"),
    )
    .show(1, False)
)

# ── Tabelas dimensionais (Fase 4) ────────────────────────────────────────
for table in ["dim_date", "dim_products", "dim_users", "fact_sales"]:
    try:
        n = spark.read.table(f"lakehouse.gold.{table}").count()
        logger.info("  gold.%-18s : %d linhas", table, n)
    except Exception:
        logger.info("  gold.%-18s : (tabela vazia ou inexistente)", table)

spark.stop()
